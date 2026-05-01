package org.k1den.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.Properties;

public class AppConfig {

    public final String kafkaBootstrap;
    public final String metricsTopic;
    public final String controlTopic;
    public final int initialIntervalSeconds;
    public final String clientId;
    public final String controlGroupId;
    public final Map<String, String> tags;
    public final String hostname;
    public final String deviceId;
    public final String deviceName;

    public final Properties kafkaProducerProps = new Properties();
    public final Properties kafkaConsumerProps = new Properties();

    private AppConfig(Map<String, Object> yamlRoot) {

        // ----- Kafka -----
        Map<String, Object> kafka = (Map<String, Object>) yamlRoot.get("kafka");
        Map<String, Object> bootstrap = (Map<String, Object>) kafka.get("bootstrap");
        Map<String, Object> topic = (Map<String, Object>) kafka.get("topic");
        Map<String, Object> producer = (Map<String, Object>) kafka.get("producer");
        Map<String, Object> client = (Map<String, Object>) kafka.get("client");
        Map<String, Object> control = (Map<String, Object>) kafka.get("control");
        Map<String, Object> controlGroup = (Map<String, Object>) control.get("group");

        kafkaBootstrap = (String) bootstrap.getOrDefault("servers", "localhost:9092");
        metricsTopic = (String) topic.getOrDefault("metrics", "metrics");
        controlTopic = (String) topic.getOrDefault("control", "metrics-control");
        clientId = (String) client.getOrDefault("id", "metrics-collector");
        controlGroupId = (String) controlGroup.getOrDefault("id", "metrics-collector-control-group");

        // ----- Collector -----
        Map<String, Object> collector = (Map<String, Object>) yamlRoot.get("collector");
        Map<String, Object> interval = (Map<String, Object>) collector.get("interval");
        initialIntervalSeconds = ((Number) interval.getOrDefault("seconds", 10)).intValue();

        // ----- App -----
        Map<String, Object> app = (Map<String, Object>) yamlRoot.get("app");
        Map<String, Object> device = (Map<String, Object>) app.getOrDefault("device", Collections.emptyMap());

        String devId = (String) device.getOrDefault("id", "");
        if (devId == null || devId.isBlank()) {
            devId = generateDeviceId();
        } else {
            devId = resolveEnvironmentVariable(devId);
        }
        deviceId = devId;

        String devName = (String) device.getOrDefault("name", "unknown-device");
        deviceName = resolveEnvironmentVariable(devName);

        String hn = (String) app.getOrDefault("hostname", "");
        if (hn == null || hn.isBlank()) {
            String resolved = "unknown-host";
            try {
                resolved = InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {}
            hostname = resolved;
        } else {
            hostname = hn;
        }

        tags = toStringMap((Map<String, Object>) app.getOrDefault("tags", Collections.emptyMap()));

        // ----- Kafka producer props -----
        kafkaProducerProps.put("bootstrap.servers", kafkaBootstrap);
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put("client.id", clientId);
        kafkaProducerProps.put("acks", producer.getOrDefault("acks", "all"));
        kafkaProducerProps.put("retries", producer.getOrDefault("retries", 3));

        // ----- Kafka consumer props -----
        kafkaConsumerProps.put("bootstrap.servers", kafkaBootstrap);
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("group.id", controlGroupId);
        kafkaConsumerProps.put("auto.offset.reset", "latest");

        System.out.println("=== DEBUG: Device ID Generation ===");
        System.out.println("Resolved device_id: " + deviceId);
        System.out.println("Resolved device_name: " + deviceName);
        System.out.println("Hostname: " + hostname);
        System.out.println("================================");
    }

    private String generateDeviceId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            String mac = "";
            try {
                java.net.NetworkInterface network = java.net.NetworkInterface.getNetworkInterfaces().nextElement();
                byte[] macBytes = network.getHardwareAddress();
                if (macBytes != null) {
                    StringBuilder sb = new StringBuilder();
                    for (byte b : macBytes) {
                        sb.append(String.format("%02X", b));
                    }
                    mac = sb.toString();
                }
            } catch (Exception e) {
                mac = "unknown-mac";
            }
            return "device-" + Math.abs(host.hashCode()) + "-" + Math.abs(mac.hashCode());
        } catch (Exception e) {
            return "device-" + System.currentTimeMillis();
        }
    }

    private String resolveEnvironmentVariable(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        if (value.startsWith("${") && value.endsWith("}")) {
            String inner = value.substring(2, value.length() - 1);
            String[] parts = inner.split(":", 2);

            String varName = parts[0].trim();
            String defaultValue = parts.length > 1 ? parts[1].trim() : "";

            String envValue = System.getenv(varName);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }

            String propValue = System.getProperty(varName);
            if (propValue != null && !propValue.isEmpty()) {
                return propValue;
            }

            return defaultValue;
        }

        return value;
    }

    private Map<String, String> toStringMap(Map<String, Object> src) {
        Map<String, String> out = new LinkedHashMap<>();
        for (var e : src.entrySet()) {
            out.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return out;
    }

    public static AppConfig load() {
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) {
                throw new RuntimeException("application.yml not found in resources");
            }
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(in);
            return new AppConfig(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load YAML config: " + e.getMessage(), e);
        }
    }
}