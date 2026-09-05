package com.company.demospringkafka.controller;

import com.company.demospringkafka.dto.CreatePaymentRequest;
import com.company.demospringkafka.event.PaymentCompletedEvent;
import com.company.demospringkafka.producer.PaymentEventProducer;
import com.company.demospringkafka.store.ConsumedEventStore;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class TestPaymentController {

    private final PaymentEventProducer paymentEventProducer;
    private final ConsumedEventStore consumedEventStore;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PaymentCompletedEvent publish(@Valid @RequestBody CreatePaymentRequest request) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                request.orderId(),
                request.amount(),
                request.customerId(),
                Instant.now()
        );
        paymentEventProducer.send(event);
        return event;
    }

    @GetMapping("/consumed")
    public List<PaymentCompletedEvent> consumed() {
        return consumedEventStore.findAll();
    }
}
