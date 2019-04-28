package com.mf.orderbook;


import java.math.BigDecimal;
import java.math.RoundingMode;

public class OrderBook implements Level2View {


    private static final int DEFAULT_PRICE_SCALE = 4;

    private final OrderBookSide bids;
    private final OrderBookSide asks;
    private final int priceScale;


    public OrderBook(){
        this(DEFAULT_PRICE_SCALE);
    }


    public OrderBook(int priceScale) {
        bids = new OrderBookSide(Side.BID);
        asks = new OrderBookSide(Side.ASK);
        this.priceScale = priceScale;
    }


    @Override
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        selectOrderBookSide(side).createOrder(side, normalizePrice(price), quantity, orderId);
    }


    @Override
    public void onCancelOrder(long orderId) {
        if(!bids.cancelOrder(orderId)){
            asks.cancelOrder(orderId);
        }
    }


    @Override
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        BigDecimal normalizedPrice = normalizePrice(price);
        if(!bids.replaceOrder(normalizedPrice, quantity, orderId)){
            asks.replaceOrder(normalizedPrice, quantity, orderId);
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
        return selectOrderBookSide(side).getPriceLevelSize(normalizePrice(price));
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


    private BigDecimal normalizePrice(BigDecimal price){
        return price.setScale(priceScale, RoundingMode.UNNECESSARY);
    }
}
