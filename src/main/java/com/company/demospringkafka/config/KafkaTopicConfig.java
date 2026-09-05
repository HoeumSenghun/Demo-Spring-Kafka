package com.company.demospringkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentsCompletedTopic() {
        return TopicBuilder.name(KafkaTopics.PAYMENTS_COMPLETED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentsCompletedDltTopic() {
        return TopicBuilder.name(KafkaTopics.PAYMENTS_COMPLETED_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
