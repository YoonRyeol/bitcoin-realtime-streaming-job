package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.io.InputStream;
import java.time.Duration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.yaml.snakeyaml.Yaml;
import java.util.Map;

public class JobConfig {
    public final String kafkaBootstrapServers;
    public final String kafkaTopic;
    public final String kafkaGroupId;
    public final OffsetsInitializer kafkaStartOffset;
    public final Duration watermarkDelay;
    public final Duration windowSize;
    public final Duration windowSlide;
    public final long checkpointIntervalMs;
    public final CheckpointingMode checkpointMode;
    public final int topN;
    public final int parallelism;
    public final Duration dedupTtl;

    public JobConfig() {
        // Load from classpath resource, with env var overrides
        Map<String, Object> config = loadYaml();

        Map<String, Object> kafka = getMap(config, "kafka");
        this.kafkaBootstrapServers = getEnvOrConfig("KAFKA_BOOTSTRAP_SERVERS", kafka, "bootstrap_servers", "kafka-kafka.kafka.svc.cluster.local:9092");
        this.kafkaTopic = getEnvOrConfig("KAFKA_TOPIC", kafka, "topic", "upbit-trades");
        this.kafkaGroupId = getEnvOrConfig("KAFKA_GROUP_ID", kafka, "group_id", "bitcoin-realtime-streaming-job");
        String offsetStr = getEnvOrConfig("KAFKA_START_OFFSET", kafka, "start_offset", "latest");
        this.kafkaStartOffset = "earliest".equals(offsetStr) ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();

        Map<String, Object> flink = getMap(config, "flink");
        this.watermarkDelay = Duration.ofSeconds(Long.parseLong(getEnvOrConfig("WATERMARK_DELAY_SEC", flink, "watermark_delay_sec", "5")));
        this.windowSize = Duration.ofMinutes(Long.parseLong(getEnvOrConfig("WINDOW_SIZE_MIN", flink, "window_size_min", "5")));
        this.windowSlide = Duration.ofMinutes(Long.parseLong(getEnvOrConfig("WINDOW_SLIDE_MIN", flink, "window_slide_min", "1")));
        this.checkpointIntervalMs = Long.parseLong(getEnvOrConfig("CHECKPOINT_INTERVAL_MS", flink, "checkpoint_interval_ms", "60000"));
        this.topN = Integer.parseInt(getEnvOrConfig("TOP_N", flink, "top_n", "20"));
        this.parallelism = Integer.parseInt(getEnvOrConfig("PARALLELISM", flink, "parallelism", "1"));
        this.checkpointMode = CheckpointingMode.EXACTLY_ONCE;

        Map<String, Object> dedup = getMap(config, "dedup");
        this.dedupTtl = Duration.ofMinutes(Long.parseLong(getEnvOrConfig("DEDUP_TTL_MIN", dedup, "ttl_min", "10")));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadYaml() {
        try {
            Yaml yaml = new Yaml();
            InputStream is = getClass().getClassLoader().getResourceAsStream("config.yaml");
            if (is != null) {
                return yaml.load(is);
            }
        } catch (Exception e) {
            // Fall through to defaults
        }
        return Map.of();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(Map<String, Object> config, String key) {
        Object val = config.get(key);
        if (val instanceof Map) {
            return (Map<String, Object>) val;
        }
        return Map.of();
    }

    private String getEnvOrConfig(String envKey, Map<String, Object> config, String configKey, String defaultValue) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) {
            return envVal;
        }
        Object configVal = config.get(configKey);
        if (configVal != null) {
            return configVal.toString();
        }
        return defaultValue;
    }
}
