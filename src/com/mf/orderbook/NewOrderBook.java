package com.mf.orderbook;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


public class NewOrderBook implements Level2View {

    private static final int DEFAULT_PRICE_SCALE = 4;
    private static Comparator<PriceLevel> PRICE_LEVEL_ASCENDING = (p1, p2) -> p1.getPrice().compareTo(p2.getPrice());
    private static Comparator<PriceLevel> PRICE_LEVEL_DESCENDING = (p1, p2) -> -p1.getPrice().compareTo(p2.getPrice());


    private ExecutorService executor;
    private final int priceScale;
    private final Map<Long, Order> orderMap;
    private AtomicLong bidPriceLevelsDepth;
    private AtomicLong askPriceLevelsDepth;
    private final SortedSet<PriceLevel> bidPriceLevels;
    private final SortedSet<PriceLevel> askPriceLevels;
    private final Map<BigDecimal, PriceLevel> bidPriceLevelMap;
    private final Map<BigDecimal, PriceLevel> askPriceLevelMap;


    public NewOrderBook() {
        this(DEFAULT_PRICE_SCALE);
    }


    public NewOrderBook(int priceScale) {
        this.priceScale = priceScale;
        orderMap = new ConcurrentHashMap<>();
        bidPriceLevelsDepth = new AtomicLong(0);
        askPriceLevelsDepth = new AtomicLong(0);
        bidPriceLevels = new ConcurrentSkipListSet<>(PRICE_LEVEL_DESCENDING);
        askPriceLevels = new ConcurrentSkipListSet<>(PRICE_LEVEL_ASCENDING);
        bidPriceLevelMap = new ConcurrentHashMap<>();
        askPriceLevelMap = new ConcurrentHashMap<>();
        executor = Executors.newSingleThreadExecutor();
    }


    @Override
    public void onNewOrder(Side side, BigDecimal price, long quantity, long orderId) {
        new NewOrderTask(side, normalizePrice(price), quantity, orderId).run();
        //executor.submit(new NewOrderTask(side, normalizePrice(price), quantity, orderId));
    }

    @Override
    public void onCancelOrder(long orderId) {
        new CancelOrderTask(orderId).run();
        //executor.submit(new CancelOrderTask(orderId));
    }

    @Override
    public void onReplaceOrder(BigDecimal price, long quantity, long orderId) {
        new ReplaceOrderTask(normalizePrice(price), quantity, orderId).run();
        //executor.submit(new ReplaceOrderTask(normalizePrice(price), quantity, orderId));
    }

    @Override
    public void onTrade(long quantity, long restingOrderId) {
        new FillOrderTask(quantity, restingOrderId).run();
        //executor.submit(new FillOrderTask(quantity, restingOrderId));
    }

    @Override
    public long getSizeForPriceLevel(Side side, BigDecimal price) {
        Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
        PriceLevel priceLevel = priceLevelMap.get(normalizePrice(price));
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



    //
    // OrderBookUpdateTask
    //

    abstract class OrderBookUpdateTask implements Runnable {

        protected void addPriceLevel(PriceLevel priceLevel, Side side) {
            if (side == Side.BID) {
                addPriceLevel(bidPriceLevels, bidPriceLevelMap, bidPriceLevelsDepth, priceLevel);
            } else {
                addPriceLevel(askPriceLevels, askPriceLevelMap, askPriceLevelsDepth, priceLevel);
            }
        }

        protected void removePriceLevel(PriceLevel priceLevel, Side side) {
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


    //
    // NewOrderTask
    //

    class NewOrderTask extends OrderBookUpdateTask {

        private Side side;
        private BigDecimal price;
        private long quantity;
        private long orderId;

        NewOrderTask(Side side, BigDecimal price, long quantity, long orderId) {
            this.side = side;
            this.price = price;
            this.quantity = quantity;
            this.orderId = orderId;
        }

        @Override
        public void run() {
            Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
            PriceLevel priceLevel = priceLevelMap.get(price);
            Order order = new Order(side, price, quantity, orderId);
            orderMap.put(orderId, order);
            if (priceLevel == null) {
                priceLevel = new PriceLevel(price, quantity);
                order.setPriceLevel(priceLevel);
                addPriceLevel(priceLevel, side);
            } else {
                order.setPriceLevel(priceLevel);
                priceLevel.getTotalOrderQuantity().getAndAdd(quantity);
            }
        }

    }

    //
    // CancelOrderTask
    //

    class CancelOrderTask extends OrderBookUpdateTask {

        private long orderId;

        CancelOrderTask(long orderId) {
            this.orderId = orderId;
        }

        @Override
        public void run() {
            Order order = orderMap.remove(orderId);
            if (order == null) return;
            order.getPriceLevel().updateTotalOrderQuantity(-order.getQuantity());
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                removePriceLevel(order.getPriceLevel(), order.getSide());
            }
        }

    }

    //
    // ReplaceOrderTask
    //

    private class ReplaceOrderTask extends OrderBookUpdateTask {

        private BigDecimal price;
        private long quantity;
        private long orderId;

        ReplaceOrderTask(BigDecimal price, long quantity, long orderId) {
            this.price = price;
            this.quantity = quantity;
            this.orderId = orderId;
        }

        @Override
        public void run() {
            Order order = orderMap.remove(orderId);
            if (order == null) return;
            order.getPriceLevel().updateTotalOrderQuantity(-order.getQuantity());
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                removePriceLevel(order.getPriceLevel(), order.getSide());
            }
            Side side = order.getSide();
            Map<BigDecimal, PriceLevel> priceLevelMap = side == Side.BID ? bidPriceLevelMap : askPriceLevelMap;
            PriceLevel priceLevel = priceLevelMap.get(price);
            order = new Order(side, price, quantity, orderId);
            orderMap.put(orderId, order);
            if (priceLevel == null) {
                priceLevel = new PriceLevel(price, quantity);
                order.setPriceLevel(priceLevel);
                addPriceLevel(priceLevel, side);
            } else {
                order.setPriceLevel(priceLevel);
                priceLevel.getTotalOrderQuantity().getAndAdd(quantity);
            }
        }
    }

    //
    // FillOrderTask
    //

    class FillOrderTask extends OrderBookUpdateTask {

        private long quantity;
        private long orderId;

        FillOrderTask(long quantity, long orderId) {
            this.quantity = quantity;
            this.orderId = orderId;
        }

        @Override
        public void run() {
            Order order = orderMap.get(orderId);
            if (order == null) return;
            order.getPriceLevel().updateTotalOrderQuantity(-quantity);
            order.updateQuantity(-quantity);
            if(order.getQuantity() == 0){
                orderMap.remove(orderId);
            }
            if (order.getPriceLevel().getTotalOrderQuantity().get() == 0) {
                removePriceLevel(order.getPriceLevel(), order.getSide());
            }
        }
    }

}
