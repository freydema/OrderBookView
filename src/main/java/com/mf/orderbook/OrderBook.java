package com.mf.orderbook;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class OrderBook implements Level2View {

    private static final int DEFAULT_PRICE_SCALE = 4;
    private static Comparator<PriceLevel> PRICE_LEVEL_ASCENDING = (p1, p2) -> p1.getPrice().compareTo(p2.getPrice());
    private static Comparator<PriceLevel> PRICE_LEVEL_DESCENDING = (p1, p2) -> -p1.getPrice().compareTo(p2.getPrice());


    // Contain RW locks
    private final Map<BigDecimal, Lock> priceLevelLocks;

    private final int priceScale;
    private final Map<Long, Order> orderMap;
    private final AtomicLong bidPriceLevelsDepth;
    private final AtomicLong askPriceLevelsDepth;
    private final SortedSet<PriceLevel> bidPriceLevels;
    private final SortedSet<PriceLevel> askPriceLevels;
    private final Map<BigDecimal, PriceLevel> bidPriceLevelMap;
    private final Map<BigDecimal, PriceLevel> askPriceLevelMap;


    public OrderBook() {
        this(DEFAULT_PRICE_SCALE);
    }


    public OrderBook(int priceScale) {
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


    /* Place a new order in:
      - o(log N) if size of that price level is 0  on the given side of the book.
       N being the depth of the book for that side. This is due to the add operation on the sorted set
      - o(1) otherwise
   */
    @Override
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        // This is an arbitrary decision to ignore the order when the validation fails.
        // We could also throw a Runtime exception...
        if(side == null || price == null || price.signum() <=0 || quantity <= 0) return;
        price = normalizePrice(price);
        Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
        Lock priceLevelLock = getPriceLevelLock(price);
        priceLevelLock.lock();
        try {
            Order order = new Order(side, price, quantity, orderId);
            PriceLevel priceLevel = priceLevelMap.get(price);
            orderMap.put(orderId, order);
            if (priceLevel == null) {
                priceLevel = new PriceLevel(price, quantity);
                order.setPriceLevel(priceLevel);
                if (order.getSide() == Side.BID) {
                    bidPriceLevelMap.put(priceLevel.getPrice(), priceLevel);
                    bidPriceLevels.add(priceLevel);
                    bidPriceLevelsDepth.getAndIncrement();
                } else {
                    askPriceLevelMap.put(priceLevel.getPrice(), priceLevel);
                    askPriceLevels.add(priceLevel);
                    askPriceLevelsDepth.getAndIncrement();
                }
            } else {
                order.setPriceLevel(priceLevel);
                priceLevel.getTotalOrderQuantity().getAndAdd(quantity);
            }
        } finally {
            priceLevelLock.unlock();
        }
    }

    /* Cancel an order in:
       - o(log N) if the total quantity for the price level and side is 0 after that cancel.
        N being the depth of the book for that side. This is due to the remove operation on the sorted set
       - o(1) otherwise
    */
    @Override
    public void onCancelOrder(long orderId) {
        Order order = orderMap.remove(orderId);
        if (order == null) return;
        Lock priceLevelLock = getPriceLevelLock(order.getPrice());
        priceLevelLock.lock();
        try {
            PriceLevel priceLevel = order.getPriceLevel();
            priceLevel.updateTotalOrderQuantity(-order.getQuantity());
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                if (order.getSide() == Side.BID) {
                    bidPriceLevelMap.remove(priceLevel.getPrice());
                    bidPriceLevels.remove(priceLevel); // o(log N) operation
                    bidPriceLevelsDepth.getAndDecrement();
                } else {
                    askPriceLevelMap.remove(priceLevel.getPrice());
                    askPriceLevels.remove(priceLevel);
                    askPriceLevelsDepth.getAndDecrement();
                }
            }
        } finally {
            priceLevelLock.unlock();
        }
    }

    /* Replace an order in:
        - o(1) best case
        - o(2 * log N) = o(log N) worst case
       See details on the called method comments
    */
    @Override
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        if(price == null || price.signum() <=0 || quantity <= 0) return;
        price = normalizePrice(price);
        Order order = orderMap.get(orderId);
        if(order == null) return;
        onCancelOrder(orderId);
        onNewOrder(order.getSide(), price, quantity, orderId);
    }

    /* Fill an order in:
        - o(log N) if the total quantity for the price level and side is 0 after that trade.
         N being the depth of the book for that side. This is due to the remove operation on the sorted set
        - o(1) otherwise
    */
    @Override
    public void onTrade(long quantity, long restingOrderId) {
        if(quantity <= 0) return;
        Order order = orderMap.get(restingOrderId);
        if (order == null) return;
        Lock priceLevelLock = getPriceLevelLock(order.getPrice());
        priceLevelLock.lock();
        try {
            PriceLevel priceLevel = order.getPriceLevel();
            priceLevel.updateTotalOrderQuantity(-quantity);
            order.updateQuantity(-quantity);
            if(order.getQuantity() == 0){
                orderMap.remove(restingOrderId);
            }
            if (priceLevel.getTotalOrderQuantity().get() == 0) {
                if (order.getSide() == Side.BID) {
                    bidPriceLevelMap.remove(priceLevel.getPrice());
                    bidPriceLevels.remove(priceLevel);
                    bidPriceLevelsDepth.getAndDecrement();
                } else {
                    askPriceLevelMap.remove(priceLevel.getPrice());
                    askPriceLevels.remove(priceLevel);
                    askPriceLevelsDepth.getAndDecrement();
                }
            }
        } finally {
            priceLevelLock.unlock();
        }
    }

    // Returns the total volume on the given side and price of the book in o(1) time
    @Override
    public long getSizeForPriceLevel(Side side, BigDecimal price) {
        price = normalizePrice(price);
        Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
        PriceLevel priceLevel = priceLevelMap.get(price);
        return priceLevel == null ? 0 : priceLevel.getTotalOrderQuantity().get();
    }

    // Returns the book depth on the given side of the book in o(1) time
    @Override
    public long getBookDepth(Side side) {
        return side == Side.BID ? bidPriceLevelsDepth.get() : askPriceLevelsDepth.get();
    }

    // Returns the best price on the given side of the book in o(1) time
    @Override
    public BigDecimal getTopOfBook(Side side) {
        SortedSet<PriceLevel> priceLevels = side == Side.BID ? bidPriceLevels : askPriceLevels;
        if(priceLevels.isEmpty()) return BigDecimal.ZERO;
        return priceLevels.first().getPrice();
    }

    // Set the scale of given price to the one used by the order book. This method should be called on every public method
    // of the book that uses a price. This is to be able to use BigDecimal as a key for the different internal maps
    private BigDecimal normalizePrice(BigDecimal price) {
        return price.setScale(priceScale, RoundingMode.UNNECESSARY);
    }

    private Lock getPriceLevelLock(BigDecimal price){
        Lock lock = priceLevelLocks.get(price);
        if(lock != null) return lock;
        // If no lock was ever created, create one. Thread contention on that synchronized block should happen only the
        // rare case when two orders are created at the same time, for the same price level, for which no lock has been created yet
        synchronized (this) {
            // check again that the lock was not created by another thread that was in this synchronized block before
            lock = priceLevelLocks.get(price);
            if(lock != null) return lock;
            lock = new ReentrantLock();
            priceLevelLocks.put(price, lock);
            return lock;
        }
    }


}
