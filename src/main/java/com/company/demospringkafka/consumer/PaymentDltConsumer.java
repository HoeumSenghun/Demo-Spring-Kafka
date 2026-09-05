package com.company.demospringkafka.consumer;

import com.company.demospringkafka.config.KafkaTopics;
import com.company.demospringkafka.event.PaymentCompletedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentDltConsumer {

    @KafkaListener(topics = KafkaTopics.PAYMENTS_COMPLETED_DLT, groupId = "payment-dlt-processor")
    public void onDeadLetter(PaymentCompletedEvent event) {
        log.error("Payment {} landed in the DLT after retries failed: {}", event.paymentId(), event);
    }
}
