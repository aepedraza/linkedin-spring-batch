package com.linkedin.batch.processor;

import com.linkedin.batch.domain.TrackedOrder;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;

public class FreeShippingItemProcessor implements ItemProcessor<TrackedOrder, TrackedOrder> {

    @Override
    public TrackedOrder process(TrackedOrder item) {
        item.setFreeShipping(BigDecimal.valueOf(80.0).compareTo(item.getCost()) < 0);
        return item.isFreeShipping() ? item : null; // null indicates to filter the record
    }
}
