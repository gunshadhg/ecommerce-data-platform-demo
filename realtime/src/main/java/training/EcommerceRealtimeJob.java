package training;

import java.io.Serializable;
import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class EcommerceRealtimeJob {
    public static void main(String[] args) throws Exception {
        final String bootstrapServers =
                args.length > 0 ? args[0] : "127.0.0.1:9092";
        final String topic =
                args.length > 1 ? args[1] : "ecommerce_events";
        final String checkpointDir =
                args.length > 2
                        ? args[2]
                        : new File("checkpoints").getAbsolutePath();

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        env.getCheckpointConfig().setCheckpointStorage(
                "file:///" + checkpointDir.replace("\\", "/") + "/");

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setTopics(topic)
                        .setGroupId("flink-ecommerce-realtime-v1")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        DataStream<EcommerceEvent> events =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-events")
                        .flatMap(
                                new FlatMapFunction<String, EcommerceEvent>() {
                                    @Override
                                    public void flatMap(
                                            String json,
                                            Collector<EcommerceEvent> out) {
                                        try {
                                            out.collect(EcommerceEvent.parse(json));
                                        } catch (RuntimeException ignored) {
                                            // Invalid events are skipped for this learning job.
                                        }
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<EcommerceEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.eventTimestamp)
                                        .withIdleness(Duration.ofSeconds(5)));

        DataStream<String> metrics =
                events.filter(event -> "pay".equals(event.eventType))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new FiveMinuteGmvWindow());

        metrics.print("FIVE_MINUTE_GMV");

        KafkaSink<String> metricsSink =
                KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("realtime_gmv_metrics")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        metrics.sinkTo(metricsSink).name("realtime-gmv-kafka-sink");

        env.execute("Ecommerce Kafka to Flink 5-minute GMV");
    }

    public static class EcommerceEvent implements Serializable {
        public String eventId;
        public String eventType;
        public long userId;
        public double amount;
        public long eventTimestamp;

        public static EcommerceEvent parse(String json) {
            EcommerceEvent event = new EcommerceEvent();
            event.eventId = readString(json, "event_id");
            event.eventType = readString(json, "event_type");
            event.userId = Long.parseLong(readNumber(json, "user_id"));
            event.amount = Double.parseDouble(readNumber(json, "amount"));
            String eventTs = readString(json, "event_ts");
            event.eventTimestamp =
                    LocalDateTime.parse(
                                    eventTs,
                                    DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli();
            return event;
        }

        private static String readNumber(String json, String field) {
            String marker = "\"" + field + "\":";
            int start = json.indexOf(marker);
            if (start < 0) {
                throw new IllegalArgumentException("Missing field: " + field);
            }
            start += marker.length();
            int end = start;
            while (end < json.length()
                    && json.charAt(end) != ','
                    && json.charAt(end) != '}') {
                end++;
            }
            return json.substring(start, end).trim();
        }

        private static String readString(String json, String field) {
            String marker = "\"" + field + "\":";
            int start = json.indexOf(marker);
            if (start < 0) {
                throw new IllegalArgumentException("Missing field: " + field);
            }
            start = json.indexOf('"', start + marker.length()) + 1;
            int end = json.indexOf('"', start);
            if (start <= 0 || end < 0) {
                throw new IllegalArgumentException("Invalid string field: " + field);
            }
            return json.substring(start, end);
        }
    }

    public static class FiveMinuteGmvWindow
            implements AllWindowFunction<EcommerceEvent, String, TimeWindow> {
        @Override
        public void apply(
                TimeWindow window,
                Iterable<EcommerceEvent> events,
                Collector<String> out) {
            long paidOrders = 0L;
            double gmv = 0.0;
            Set<Long> buyers = new HashSet<>();

            for (EcommerceEvent event : events) {
                paidOrders++;
                gmv += event.amount;
                buyers.add(event.userId);
            }

            String windowStart =
                    DateTimeFormatter.ISO_INSTANT.format(
                            java.time.Instant.ofEpochMilli(window.getStart()));
            String windowEnd =
                    DateTimeFormatter.ISO_INSTANT.format(
                            java.time.Instant.ofEpochMilli(window.getEnd()));

            out.collect(
                    String.format(
                            "{\"windowStart\":\"%s\",\"windowEnd\":\"%s\",\"paidOrders\":%d,\"gmv\":%.2f,\"buyers\":%d}",
                            windowStart,
                            windowEnd,
                            paidOrders,
                            gmv,
                            buyers.size()));
        }
    }
}
