package com.mf.orderbook;

import java.math.BigDecimal;

public class OrderBook implements Level2View {

    private final OrderBookSide bids;
    private final OrderBookSide asks;

    public OrderBook() {
        bids = new OrderBookSide(Side.BID);
        asks = new OrderBookSide(Side.ASK);
    }

    //TODO normalize scale of BigDecimal so that they can be used as keys for map


    @Override
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        selectOrderBookSide(side).createOrder(side, price, quantity, orderId);
    }

    @Override
    public void onCancelOrder(long orderId) {
        if(!bids.cancelOrder(orderId)){
            asks.cancelOrder(orderId);
        }
    }

    @Override
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        if(!bids.replaceOrder(price, quantity, orderId)){
            asks.replaceOrder(price, quantity, orderId);
        }
    }

    @Override
    public void onTrade(long quantity, long restingOrderId) {
        if(!bids.fillOrder(quantity, restingOrderId)){
            asks.fillOrder(quantity, restingOrderId);
        }
    }

    @Override
    public long getSizeForPriceLevel(Side side, BigDecimal price) {
        return selectOrderBookSide(side).getPriceLevelSize(price);
    }

    @Override
    public long getBookDepth(Side side) {
        return selectOrderBookSide(side).getDepth();
    }

    @Override
    public BigDecimal getTopOfBook(Side side) {
        return selectOrderBookSide(side).getTop();
    }

    private OrderBookSide selectOrderBookSide(Side side){
        return  side == Side.BID ? bids : asks;
    }

}
