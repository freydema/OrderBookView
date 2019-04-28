package com.mf.orderbook;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class OrderBook3 implements Level2View {

    private static final int DEFAULT_PRICE_SCALE = 4;
    private static Comparator<PriceLevel> PRICE_LEVEL_ASCENDING = (p1, p2) -> p1.getPrice().compareTo(p2.getPrice());
    private static Comparator<PriceLevel> PRICE_LEVEL_DESCENDING = (p1, p2) -> -p1.getPrice().compareTo(p2.getPrice());


    private final Map<BigDecimal, ReadWriteLock> priceLevelLocks;
    private final int priceScale;
    private final Map<Long, Order> orderMap;
    private final AtomicLong bidPriceLevelsDepth;
    private final AtomicLong askPriceLevelsDepth;
    private final SortedSet<PriceLevel> bidPriceLevels;
    private final SortedSet<PriceLevel> askPriceLevels;
    private final Map<BigDecimal, PriceLevel> bidPriceLevelMap;
    private final Map<BigDecimal, PriceLevel> askPriceLevelMap;


    public OrderBook3() {
        this(DEFAULT_PRICE_SCALE);
    }


    public OrderBook3(int priceScale) {
        this.priceScale = priceScale;
        priceLevelLocks = new ConcurrentHashMap<>();
        orderMap = new ConcurrentHashMap<>();
        bidPriceLevelsDepth = new AtomicLong(0);
        askPriceLevelsDepth = new AtomicLong(0);
        bidPriceLevels = new ConcurrentSkipListSet<>(PRICE_LEVEL_DESCENDING);
        askPriceLevels = new ConcurrentSkipListSet<>(PRICE_LEVEL_ASCENDING);
        bidPriceLevelMap = new ConcurrentHashMap<>();
        askPriceLevelMap = new ConcurrentHashMap<>();
    }


    @Override
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        price = normalizePrice(price);
        Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
        ReadWriteLock lock = getPriceLevelLock(price);
        lock.writeLock().lock();
        try {
            Order order = new Order(side, price, quantity, orderId);
            PriceLevel priceLevel = priceLevelMap.get(price);
            orderMap.put(orderId, order);
            if (priceLevel == null) {
                priceLevel = new PriceLevel(price, quantity);
                order.setPriceLevel(priceLevel);
                addPriceLevel(priceLevel, side);
            } else {
                order.setPriceLevel(priceLevel);
                priceLevel.getTotalOrderQuantity().getAndAdd(quantity);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onCancelOrder(long orderId) {
        Order order = orderMap.remove(orderId);
        if (order == null) return;
        ReadWriteLock priceLevelLock = getPriceLevelLock(order.getPrice());
        priceLevelLock.writeLock().lock();
        try {
            order.getPriceLevel().updateTotalOrderQuantity(-order.getQuantity());
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                removePriceLevel(order.getPriceLevel(), order.getSide());
            }
        } finally {
            priceLevelLock.writeLock().unlock();
        }
    }

    @Override
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        Order order = orderMap.get(orderId);
        onCancelOrder(orderId);
        onNewOrder(order.getSide(), price, quantity, orderId);
    }

    @Override
    public void onTrade(long quantity, long restingOrderId) {
        Order order = orderMap.get(restingOrderId);
        if (order == null) return;
        ReadWriteLock priceLevelLock = getPriceLevelLock(order.getPrice());
        priceLevelLock.writeLock().lock();
        try {
            order.getPriceLevel().updateTotalOrderQuantity(-quantity);
            order.updateQuantity(-quantity);
            if(order.getQuantity() == 0){
                orderMap.remove(restingOrderId);
            }
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                removePriceLevel(order.getPriceLevel(), order.getSide());
            }
        } finally {
            priceLevelLock.writeLock().unlock();
        }
    }

    @Override
    public long getSizeForPriceLevel(Side side, BigDecimal price) {
        price = normalizePrice(price);
        Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
        PriceLevel priceLevel = priceLevelMap.get(price);
        return priceLevel == null ? 0 : priceLevel.getTotalOrderQuantity().get();
    }

    @Override
    public long getBookDepth(Side side) {
        return side == Side.BID ? bidPriceLevelsDepth.get() : askPriceLevelsDepth.get();
    }

    @Override
    public BigDecimal getTopOfBook(Side side) {
        SortedSet<PriceLevel> priceLevels = side == Side.BID ? bidPriceLevels : askPriceLevels;
        if(priceLevels.isEmpty()) return BigDecimal.ZERO;
        return priceLevels.first().getPrice();
    }

    private BigDecimal normalizePrice(BigDecimal price) {
        return price.setScale(priceScale, RoundingMode.UNNECESSARY);
    }

    private ReadWriteLock getPriceLevelLock(BigDecimal price){
        ReadWriteLock lock = priceLevelLocks.get(price);
        if(lock != null) return lock;
        // If no lock was ever created, create one.
        synchronized (this) {
            // check again that the lock was not created by another thread that was in this synchronized block before
            lock = priceLevelLocks.get(price);
            if(lock != null) return lock;
            lock = new ReentrantReadWriteLock();
            priceLevelLocks.put(price, lock);
            return lock;
        }
    }


    private void addPriceLevel(PriceLevel priceLevel, Side side) {
        if (side == Side.BID) {
            addPriceLevel(bidPriceLevels, bidPriceLevelMap, bidPriceLevelsDepth, priceLevel);
        } else {
            addPriceLevel(askPriceLevels, askPriceLevelMap, askPriceLevelsDepth, priceLevel);
        }
    }

    private void removePriceLevel(PriceLevel priceLevel, Side side) {
        if (side == Side.BID) {
            removePriceLevel(bidPriceLevels, bidPriceLevelMap, bidPriceLevelsDepth, priceLevel);
        } else {
            removePriceLevel(askPriceLevels, askPriceLevelMap, askPriceLevelsDepth, priceLevel);
        }
    }

    private void addPriceLevel(SortedSet<PriceLevel> priceLevels,
                               Map<BigDecimal, PriceLevel> priceLevelMap,
                               AtomicLong priceLevelDepth,
                               PriceLevel priceLevel) {
        priceLevelMap.put(priceLevel.getPrice(), priceLevel);
        priceLevels.add(priceLevel);
        priceLevelDepth.getAndIncrement();
    }

    private void removePriceLevel(SortedSet<PriceLevel> priceLevels,
                                  Map<BigDecimal, PriceLevel> priceLevelMap,
                                  AtomicLong priceLevelDepth,
                                  PriceLevel priceLevel) {
        priceLevelMap.remove(priceLevel.getPrice());
        priceLevels.remove(priceLevel);
        priceLevelDepth.getAndDecrement();
    }


}
