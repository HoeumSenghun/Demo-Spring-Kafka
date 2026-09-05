package com.company.demospringkafka.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
        String paymentId,
        String orderId,
        BigDecimal amount,
        String customerId,
        Instant completedAt
) {
}
