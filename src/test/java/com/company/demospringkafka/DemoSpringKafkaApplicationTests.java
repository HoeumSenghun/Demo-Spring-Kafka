package com.company.demospringkafka;

import com.company.demospringkafka.config.KafkaTopics;
import com.company.demospringkafka.event.PaymentCompletedEvent;
import com.company.demospringkafka.producer.PaymentEventProducer;
import com.company.demospringkafka.store.ConsumedEventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {KafkaTopics.PAYMENTS_COMPLETED, KafkaTopics.PAYMENTS_COMPLETED_DLT},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class DemoSpringKafkaApplicationTests {

    @Autowired
    private PaymentEventProducer paymentEventProducer;

    @Autowired
    private ConsumedEventStore consumedEventStore;

    @BeforeEach
    void clearStore() {
        consumedEventStore.clear();
    }

    @Test
    void contextLoads() {
    }

    @Test
    void producerPublishesAndConsumerReceivesPaymentEvent() {
        PaymentCompletedEvent event = new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                "order-1001",
                new BigDecimal("49.99"),
                "customer-42",
                Instant.parse("2026-09-05T04:00:00Z")
        );

        paymentEventProducer.send(event).join();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(consumedEventStore.findAll())
                        .anyMatch(consumed -> consumed.paymentId().equals(event.paymentId()))
        );
    }
}
