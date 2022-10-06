package com.linkedin.batch.processor;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import org.springframework.batch.item.ItemProcessor;

import java.util.UUID;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

    @Override
    public TrackedOrder process(Order item) {
        TrackedOrder output = new TrackedOrder(item);
        output.setTrackingNumber(UUID.randomUUID().toString());
        return output;
    }
}
