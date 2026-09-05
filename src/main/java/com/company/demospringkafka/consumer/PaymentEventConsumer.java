package com.company.demospringkafka.consumer;

import com.company.demospringkafka.config.KafkaTopics;
import com.company.demospringkafka.event.PaymentCompletedEvent;
import com.company.demospringkafka.store.ConsumedEventStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {

    static final String FORCE_FAIL_CUSTOMER = "FORCE_FAIL";

    private final ConsumedEventStore consumedEventStore;

    @KafkaListener(topics = KafkaTopics.PAYMENTS_COMPLETED)
    public void onPaymentCompleted(
            PaymentCompletedEvent event,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info(
                "Consumed payment {} key={} from {}-{} offset {}",
                event.paymentId(),
                key,
                topic,
                partition,
                offset
        );

        if (FORCE_FAIL_CUSTOMER.equals(event.customerId())) {
            throw new IllegalStateException("Simulated processing failure for payment " + event.paymentId());
        }

        consumedEventStore.add(event);
    }
}
