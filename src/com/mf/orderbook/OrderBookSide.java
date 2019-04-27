package com.mf.orderbook;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class OrderBookSide {

    private ReadWriteLock rwLock;
    private final SortedSet<PriceLevel> priceLevels; // use descending comparator
    private long depth;   // guarded by rwLock
    private final Map<BigDecimal, PriceLevel> priceLevelMap;
    private final Map<Long, Order> orderMap;
    private static Comparator<PriceLevel> ASCENDING = (p1, p2) -> p1.getPrice().compareTo(p2.getPrice());
    private static Comparator<PriceLevel> DESCENDING = (p1, p2) -> -p1.getPrice().compareTo(p2.getPrice());


    OrderBookSide(Level2View.Side side) {
        Comparator<PriceLevel> priceLevelComparator = selectPriceLevelComparator(side);
        priceLevels = new ConcurrentSkipListSet<>(priceLevelComparator);
        depth = 0;
        rwLock = new ReentrantReadWriteLock();
        priceLevelMap = new ConcurrentHashMap<>();
        orderMap = new ConcurrentHashMap<>();
    }

    private static Comparator<PriceLevel> selectPriceLevelComparator(Level2View.Side side) {
        return side == Level2View.Side.BID ? DESCENDING : ASCENDING;
    }

    void createOrder(Level2View.Side side, BigDecimal price, long quantity, long orderId) {
        PriceLevel priceLevel = priceLevelMap.get(price);
        Order order = new Order(side, price, quantity, orderId);
        orderMap.put(orderId, order);
        if (priceLevel == null) {
            priceLevel = new PriceLevel(price, quantity);
            order.setPriceLevel(priceLevel);
            addPriceLevel(priceLevel);
        } else {
            order.setPriceLevel(priceLevel);
            priceLevel.getTotalOrderQuantity().getAndAdd(quantity);
        }
    }

    boolean cancelOrder(long orderId) {
        Order order = orderMap.remove(orderId); // a second thread canceling the same order will get null and return
        if (order == null) return false;
        synchronized (order) {
            order.getPriceLevel().updateTotalOrderQuantity(-order.getQuantity());
        }
        if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
            removePriceLevel(order.getPriceLevel());
        }
        return true;
    }

    boolean replaceOrder(BigDecimal price, long quantity, long orderId) {
        Order order = orderMap.get(orderId);
        if (order == null) return false;
        cancelOrder(orderId);
        createOrder(order.getSide(),price,quantity, orderId);
        return true;
    }

    boolean fillOrder(long quantity, long orderId) {
        Order order = orderMap.get(orderId);
        if (order == null) return false;
        synchronized (order){
            order.getPriceLevel().updateTotalOrderQuantity(-quantity);
            order.updateQuantity(-quantity);
            if(order.getQuantity() == 0){
                orderMap.remove(orderId);
            }
        }
        if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
            removePriceLevel(order.getPriceLevel());
        }
        return true;
    }

    long getPriceLevelSize(BigDecimal price) {
        PriceLevel priceLevel = priceLevelMap.get(price);
        return priceLevel == null ? 0 : priceLevel.getTotalOrderQuantity().get();
    }

    long getDepth() {
        rwLock.readLock().lock();
        try {
            return depth;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    BigDecimal getTop() {
        if(priceLevels.isEmpty()) return BigDecimal.ZERO;
        return priceLevels.first().getPrice();
    }

    private void addPriceLevel(PriceLevel priceLevel){
        rwLock.writeLock().lock();
        try {
            priceLevelMap.put(priceLevel.getPrice(), priceLevel);
            priceLevels.add(priceLevel);
            depth++;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void removePriceLevel(PriceLevel priceLevel){
        rwLock.writeLock().lock();
        try {
            priceLevelMap.remove(priceLevel.getPrice());
            priceLevels.remove(priceLevel);
            depth--;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

}
