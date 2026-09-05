package com.company.demospringkafka.producer;

import com.company.demospringkafka.config.KafkaTopics;
import com.company.demospringkafka.event.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;

    public CompletableFuture<SendResult<String, PaymentCompletedEvent>> send(PaymentCompletedEvent event) {
        return kafkaTemplate.send(KafkaTopics.PAYMENTS_COMPLETED, event.paymentId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish payment {}", event.paymentId(), ex);
                        return;
                    }
                    log.info(
                            "Published payment {} to {}-{} offset {}",
                            event.paymentId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                    );
                });
    }
}
