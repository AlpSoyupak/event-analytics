package com.eventanalytics.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Three stream processing jobs running in one topology:
 *
 *   1. Tenant event counts  — counts all events per tenant in 1-minute tumbling
 *      windows and emits results to `events.metrics`.
 *
 *   2. Event-type counts    — counts events broken down by (tenant, event_type)
 *      in the same windows and emits to `events.metrics`.
 *
 *   3. Spike alerts         — re-uses the tenant count KTable; whenever a tenant
 *      exceeds SPIKE_THRESHOLD events in a single window an alert is emitted
 *      to `events.alerts`.
 */
public class EventStreamsApp {

    private static final Logger log = LoggerFactory.getLogger(EventStreamsApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int SPIKE_THRESHOLD = 50;

    public static void main(String[] args) throws InterruptedException {
        Properties props = buildProps();
        Topology topology = buildTopology();

        log.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Uncaught exception in thread {}", thread.getName(), throwable);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, closing streams...");
            streams.close(Duration.ofSeconds(10));
        }));

        streams.start();
        log.info("Kafka Streams started — consuming from topic 'events'");

        Thread.currentThread().join();
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> events = builder.stream(
            "events",
            Consumed.with(Serdes.String(), Serdes.String())
        );

        TimeWindows oneMinute = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        // --- 1. Per-tenant event counts ---
        KTable<Windowed<String>, Long> tenantCounts = events
            .groupBy(
                (key, value) -> extractField(value, "tenant_id"),
                Grouped.with(Serdes.String(), Serdes.String())
            )
            .windowedBy(oneMinute)
            .count(Materialized.as("tenant-event-counts"));

        tenantCounts
            .toStream()
            .map((wk, count) -> KeyValue.pair(
                wk.key(),
                buildJson("tenant_event_count", wk.key(), null, wk.window().start(), wk.window().end(), count)
            ))
            .to("events.metrics", Produced.with(Serdes.String(), Serdes.String()));

        // --- 2. Per-tenant per-event-type counts ---
        events
            .groupBy(
                (key, value) -> extractTenantAndType(value),
                Grouped.with(Serdes.String(), Serdes.String())
            )
            .windowedBy(oneMinute)
            .count(Materialized.as("tenant-type-event-counts"))
            .toStream()
            .map((wk, count) -> {
                String[] parts = wk.key().split("\\|", 2);
                String tenantId  = parts.length > 0 ? parts[0] : "unknown";
                String eventType = parts.length > 1 ? parts[1] : "unknown";
                return KeyValue.pair(
                    wk.key(),
                    buildJson("event_type_count", tenantId, eventType, wk.window().start(), wk.window().end(), count)
                );
            })
            .to("events.metrics", Produced.with(Serdes.String(), Serdes.String()));

        // --- 3. Spike alerts ---
        tenantCounts
            .toStream()
            .filter((wk, count) -> count != null && count >= SPIKE_THRESHOLD)
            .map((wk, count) -> {
                ObjectNode alert = MAPPER.createObjectNode();
                alert.put("type",            "spike_alert");
                alert.put("tenant_id",       wk.key());
                alert.put("count",           count);
                alert.put("threshold",       SPIKE_THRESHOLD);
                alert.put("window_start_ms", wk.window().start());
                alert.put("window_end_ms",   wk.window().end());
                alert.put("detected_at",     Instant.now().toString());
                return KeyValue.pair(wk.key(), alert.toString());
            })
            .to("events.alerts", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // --- helpers ---

    private static Properties buildProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,         "event-analytics-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,        "1000");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,        "2");
        return props;
    }

    private static String extractField(String json, String field) {
        try {
            return MAPPER.readTree(json).path(field).asText("unknown");
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String extractTenantAndType(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return node.path("tenant_id").asText("unknown")
                + "|"
                + node.path("event_type").asText("unknown");
        } catch (Exception e) {
            return "unknown|unknown";
        }
    }

    private static String buildJson(
        String type, String tenantId, String eventType,
        long windowStartMs, long windowEndMs, long count
    ) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("type",            type);
        out.put("tenant_id",       tenantId);
        out.put("window_start_ms", windowStartMs);
        out.put("window_end_ms",   windowEndMs);
        out.put("count",           count);
        if (eventType != null) out.put("event_type", eventType);
        return out.toString();
    }
}
