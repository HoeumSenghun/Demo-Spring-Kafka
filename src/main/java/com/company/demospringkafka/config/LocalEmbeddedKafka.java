package com.company.demospringkafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

public final class LocalEmbeddedKafka {

    private static final Logger log = LoggerFactory.getLogger(LocalEmbeddedKafka.class);

    private LocalEmbeddedKafka() {
    }

    public static boolean isEnabled(String[] args) {
        String enabled = System.getProperty("app.kafka.embedded", "true");
        for (String arg : args) {
            if (arg.startsWith("--app.kafka.embedded=")) {
                enabled = arg.substring("--app.kafka.embedded=".length());
            }
        }
        return Boolean.parseBoolean(enabled);
    }

    public static EmbeddedKafkaBroker start() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaKraftBroker(
                1,
                3,
                KafkaTopics.PAYMENTS_COMPLETED,
                KafkaTopics.PAYMENTS_COMPLETED_DLT
        ).brokerListProperty("spring.kafka.bootstrap-servers");
        broker.afterPropertiesSet();
        log.info("Embedded Kafka started at {}", broker.getBrokersAsString());
        return broker;
    }
}
