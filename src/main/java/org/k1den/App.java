package org.k1den;

import org.k1den.config.AppConfig;
import org.k1den.kafka.ControlConsumer;
import org.k1den.kafka.KafkaProducerService;
import org.k1den.service.MetricsCollectorService;

public class App {
    public static void main(String[] args) {
        AppConfig config = AppConfig.load(); // из application.properties

        KafkaProducerService producer = new KafkaProducerService(config);
        MetricsCollectorService collector = new MetricsCollectorService(config, producer);

        // стартуем consumer для управления интервалом
        ControlConsumer controlConsumer = new ControlConsumer(config, collector);
        controlConsumer.start();

        // старт основного цикла сбора
        collector.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            controlConsumer.shutdown();
            collector.shutdown();
            producer.close();
            System.out.println("Shutdown complete");
        }));
    }
}
