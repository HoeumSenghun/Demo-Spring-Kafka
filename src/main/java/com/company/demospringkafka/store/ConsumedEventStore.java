package com.company.demospringkafka.store;

import com.company.demospringkafka.event.PaymentCompletedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class ConsumedEventStore {

    private final ConcurrentLinkedDeque<PaymentCompletedEvent> events = new ConcurrentLinkedDeque<>();

    public void add(PaymentCompletedEvent event) {
        events.addFirst(event);
        while (events.size() > 50) {
            events.removeLast();
        }
    }

    public List<PaymentCompletedEvent> findAll() {
        return new ArrayList<>(events);
    }

    public void clear() {
        events.clear();
    }
}
