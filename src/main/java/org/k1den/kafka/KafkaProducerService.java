package org.k1den.kafka;

import org.apache.kafka.clients.producer.*;
import org.k1den.config.AppConfig;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerService {
    private final Producer<String, String> producer;
    private final String metricsTopic;

    public KafkaProducerService(AppConfig cfg) {
        Properties p = cfg.kafkaProducerProps;
        this.producer = new KafkaProducer<>(p);
        this.metricsTopic = cfg.metricsTopic;
    }

    public Future<RecordMetadata> send(String key, String value) {
        ProducerRecord<String, String> rec = new ProducerRecord<>(metricsTopic, key, value);
        return producer.send(rec, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send metric: " + exception.getMessage());
            }
        });
    }

    public void close() {
        try {
            producer.flush();
            producer.close();
        } catch (Exception ignored) {}
    }
}

