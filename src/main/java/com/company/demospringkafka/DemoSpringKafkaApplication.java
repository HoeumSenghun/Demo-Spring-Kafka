package com.company.demospringkafka;

import com.company.demospringkafka.config.LocalEmbeddedKafka;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@SpringBootApplication
public class DemoSpringKafkaApplication {

    public static void main(String[] args) {
        EmbeddedKafkaBroker embeddedKafka = null;
        if (LocalEmbeddedKafka.isEnabled(args)) {
            embeddedKafka = LocalEmbeddedKafka.start();
            EmbeddedKafkaBroker brokerToStop = embeddedKafka;
            Runtime.getRuntime().addShutdownHook(new Thread(brokerToStop::destroy, "embedded-kafka-shutdown"));
        }
        SpringApplication.run(DemoSpringKafkaApplication.class, args);
    }

}
