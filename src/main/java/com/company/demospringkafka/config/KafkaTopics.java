package com.company.demospringkafka.config;

public final class KafkaTopics {

    public static final String PAYMENTS_COMPLETED = "payments.completed";
    public static final String PAYMENTS_COMPLETED_DLT = PAYMENTS_COMPLETED + ".DLT";

    private KafkaTopics() {
    }
}
