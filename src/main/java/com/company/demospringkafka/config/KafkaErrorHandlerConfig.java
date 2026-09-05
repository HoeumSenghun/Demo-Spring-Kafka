package com.company.demospringkafka.config;

import com.company.demospringkafka.event.PaymentCompletedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate) {
        DeadLetterPublishingRecoverer dltRecoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(KafkaTopics.PAYMENTS_COMPLETED_DLT, record.partition())
        );

        return new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception exception) -> {
                    if (KafkaTopics.PAYMENTS_COMPLETED_DLT.equals(record.topic())) {
                        log.error("Dropping failed DLT record at offset {}", record.offset(), exception);
                        return;
                    }
                    dltRecoverer.accept(record, exception);
                },
                new FixedBackOff(500L, 2)
        );
    }
}
