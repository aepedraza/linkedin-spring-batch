package com.linkedin.batch.domain;

import org.springframework.beans.BeanUtils;

public class TrackedOrder extends Order {

    private String trackingNumber;
    private String freeShipping;

    public TrackedOrder() {
    }

    public TrackedOrder(Order order) {
        BeanUtils.copyProperties(order, this);
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public void setTrackingNumber(String trackingNumber) {
        this.trackingNumber = trackingNumber;
    }

    public String getFreeShipping() {
        return freeShipping;
    }

    public void setFreeShipping(String freeShipping) {
        this.freeShipping = freeShipping;
    }
}
