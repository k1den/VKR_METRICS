package org.k1den.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.k1den.config.AppConfig;
import org.k1den.service.MetricsCollectorService;
import org.k1den.util.JsonUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Listens to the control topic for JSON messages like {"interval":15}
 * and updates the collector's interval. Validates 5..60 sec.
 */
public class ControlConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final MetricsCollectorService collector;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String topic;

    public ControlConsumer(AppConfig cfg, MetricsCollectorService collector) {
        this.collector = collector;
        this.topic = cfg.controlTopic;
        Properties p = cfg.kafkaConsumerProps;
        // add client id
        p.put("client.id", cfg.clientId + "-control");
        this.consumer = new KafkaConsumer<>(p);
    }

    public void start() {
        Thread t = new Thread(this::runLoop, "control-consumer");
        t.setDaemon(true);
        t.start();
    }

    private void runLoop() {
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("ControlConsumer subscribed to " + topic);
        while (running.get()) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofSeconds(1));
            recs.forEach(r -> {
                try {
                    // simple parsing to Map
                    Map map = JsonUtils.fromJson(r.value(), Map.class);
                    if (map.containsKey("interval")) {
                        int newInterval = Integer.parseInt(map.get("interval").toString());
                        collector.updateInterval(newInterval);
                        System.out.println("ControlConsumer applied new interval: " + newInterval + "s");
                    } else {
                        System.out.println("ControlConsumer unknown control message: " + r.value());
                    }
                } catch (Exception ex) {
                    System.err.println("ControlConsumer failed to parse: " + ex.getMessage());
                }
            });
        }
        consumer.close();
    }

    public void shutdown() {
        running.set(false);
    }
}

