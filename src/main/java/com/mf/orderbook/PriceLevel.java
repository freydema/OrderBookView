package com.mf.orderbook;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  @author Marc Freydefont
 */
class PriceLevel {

    private final BigDecimal price;
    private final AtomicLong totalOrderQuantity;

    PriceLevel(BigDecimal price, long initialQuantity){
        this.price = price;
        totalOrderQuantity = new AtomicLong(initialQuantity);
    }

    BigDecimal getPrice() {
        return price;
    }

    AtomicLong getTotalOrderQuantity(){
        return totalOrderQuantity;
    }

    void updateTotalOrderQuantity(long delta){
        totalOrderQuantity.getAndAdd(delta);
    }
}
