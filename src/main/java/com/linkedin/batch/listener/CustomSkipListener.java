package com.linkedin.batch.listener;

import com.linkedin.batch.domain.Order;
import com.linkedin.batch.domain.TrackedOrder;
import org.springframework.batch.core.SkipListener;

public class CustomSkipListener implements SkipListener<Order, TrackedOrder> {
    @Override
    public void onSkipInRead(Throwable t) {
        // nothing to do
    }

    @Override
    public void onSkipInWrite(TrackedOrder item, Throwable t) {
        // nothing to do
    }

    @Override
    public void onSkipInProcess(Order item, Throwable t) {
        System.out.println("Skipping processing of item with ID: " + item.getOrderId());
    }
}
