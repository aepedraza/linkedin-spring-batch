package com.linkedin.batch.processor;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import com.linkedin.batch.exception.OrderProcessingException;
import org.springframework.batch.item.ItemProcessor;

import java.util.UUID;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

    public static final double FAILING_PROBABILITY = .005;

    @Override
    public TrackedOrder process(Order item) throws OrderProcessingException {
        System.out.println("Processing order with ID: " + item.getOrderId());
        TrackedOrder output = new TrackedOrder(item);
        output.setTrackingNumber(getTrackingNumber());
        return output;
    }

    private String getTrackingNumber() throws OrderProcessingException {

        if (Math.random() < FAILING_PROBABILITY) {
            throw new OrderProcessingException();
        }

        return UUID.randomUUID().toString();
    }
}
