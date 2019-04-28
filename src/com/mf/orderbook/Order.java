package com.mf.orderbook;

import java.math.BigDecimal;
import static com.mf.orderbook.Level2View.Side;

public class Order {

    private final Side side;
    private BigDecimal price;
    private long quantity;
    private final long orderId;

    private PriceLevel priceLevel;

    Order(Side side, BigDecimal price, long quantity, long orderId) {
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.orderId = orderId;
    }

    Side getSide() {
        return side;
    }

    PriceLevel getPriceLevel() {
        return priceLevel;
    }

    void setPriceLevel(PriceLevel priceLevel) {
        this.priceLevel = priceLevel;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public long getOrderId() {
        return orderId;
    }

    long getQuantity() {
        return quantity;
    }

    void updateQuantity(long delta) {
        this.quantity = quantity + delta;
        if(quantity <= 0) quantity = 0;
    }

}
