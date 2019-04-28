package com.mf.orderbook;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import static com.mf.orderbook.Level2View.Side.*;

public class OrderBookTest {

    private static final long SEED = 1;
    private static final long ORDER_1 = 1;
    private static final long ORDER_2 = 2;
    private static final long ORDER_3 = 3;
    private static final long ORDER_4 = 4;

    private static final BigDecimal P0 = BigDecimal.ZERO;
    private static final BigDecimal P20 = BigDecimal.valueOf(20);
    private static final BigDecimal P21 = BigDecimal.valueOf(21);
    private static final BigDecimal P22 = BigDecimal.valueOf(22);
    private static final BigDecimal P23 = BigDecimal.valueOf(23);
    private static final BigDecimal P24 = BigDecimal.valueOf(24);


    @Test
    public void getBookDepthTest() {
        Level2View book = createOrderBook();
        checkBookDepth(book, 0, 0);
        // Place order 1: +1 on the BID depth
        book.onNewOrder(BID, P21, 100, ORDER_1);
        checkBookDepth(book, 1, 0);
        // Place order 2: +1 on the ASK depth
        book.onNewOrder(ASK, P22, 200, ORDER_2);
        checkBookDepth(book, 1, 1);
        // Place order 3: +1 on the ASK depth
        book.onNewOrder(ASK, P23, 300, ORDER_3);
        checkBookDepth(book, 1, 2);
        // Cancel order 1: -1 on the BID depth
        book.onCancelOrder(ORDER_1);
        checkBookDepth(book, 0, 2);
        // Replace order 2 quantity only: no change on the order book depth
        book.onReplaceOrder(P22, 210, ORDER_2);
        checkBookDepth(book, 0, 2);
        // Replace order 3: price 23 -> 22, same value as Order 2: : -1 on the ASK depth
        book.onReplaceOrder(P22, 300, ORDER_3);
        checkBookDepth(book, 0, 1);
        // Place order 4: +1 on the BID depth
        book.onNewOrder(BID, P20, 400, ORDER_4);
        checkBookDepth(book, 1, 1);
        // 1st execution for order 4, order not filled: no change on the order book depth,
        book.onTrade(300, ORDER_4);
        checkBookDepth(book, 1, 1);
        // 2nd execution for order 4, order filled: -1 on the BID depth
        book.onTrade(100, ORDER_4);
        checkBookDepth(book, 0, 1);
    }

    @Test
    public void getTopOfBookTest() {
        Level2View book = createOrderBook();
        checkTopOfBook(book, P0, P0); // or null, null ?
        // Place order 1: top bid = 21
        book.onNewOrder(BID, P21, 100, ORDER_1);
        checkTopOfBook(book, P21, P0);
        // Place order 2: top bid = 22 (highest bid is the best)
        book.onNewOrder(BID, P22, 200, ORDER_2);
        checkTopOfBook(book, P22, P0);
        // Place order 3: top ask = 24
        book.onNewOrder(ASK, P24, 300, ORDER_3);
        checkTopOfBook(book, P22, P24);
        // Place order 4: top ask = 23 (lowest ask is the best)
        book.onNewOrder(ASK, P23, 300, ORDER_4);
        checkTopOfBook(book, P22, P23);
        // Cancel order 2: top bid = 21
        book.onCancelOrder(ORDER_2);
        checkTopOfBook(book, P21, P23);
        // Replace order 1: price 21 -> 22, top bid = 22
        book.onReplaceOrder(P22, 100, ORDER_1);
        checkTopOfBook(book, P22, P23);
        // Execution for order 4, order 4 filled: top ask = 24 (order 3)
        book.onTrade(300, ORDER_4);
        checkTopOfBook(book, P22, P24);
    }

    @Test
    public void getSizeForPriceLevelTest() {
        Level2View book = createOrderBook();
        //BigDecimal P21 = Level2ViewTest.P21;
        // No order for price level: BID and ASK sizes are zero on price level 1
        book.getSizeForPriceLevel(ASK, P21);
        checkSizeForPriceLevel(book, P21, 0, 0);
        // Place order 1: +100 BID size on price 21
        book.onNewOrder(BID, P21, 100, ORDER_1);
        checkSizeForPriceLevel(book, P21, 100, 0);
        // Place order 2: +200 ASK size on price 22
        book.onNewOrder(ASK, P22, 200, ORDER_2);
        checkSizeForPriceLevel(book, P22, 0, 200);
        checkSizeForPriceLevel(book, P21, 100, 0);
        // Cancel order 1: sizes are zero on price 21
        book.onCancelOrder(ORDER_1);
        checkSizeForPriceLevel(book, P21, 0, 0);
        checkSizeForPriceLevel(book, P22, 0, 200);
        // Replace order 2: price and quantity
        book.onReplaceOrder(P23, 300, ORDER_2);
        checkSizeForPriceLevel(book, P21, 0, 0);
        checkSizeForPriceLevel(book, P22, 0, 0);
        checkSizeForPriceLevel(book, P23, 0, 300);
        // Execution for order 2, order 2 filled: sizes are zero at price 23
        book.onTrade(300, ORDER_2);
        checkSizeForPriceLevel(book, P23, 0, 0);
    }

    @Test
    public void anotherTest() {
        Level2View book = createOrderBook();
        book.onNewOrder(BID, BigDecimal.valueOf(21.0), 100, ORDER_1);
        checkTopOfBook(book, P21, P0);
        checkSizeForPriceLevel(book, P21, 100, 0);
        checkBookDepth(book, 1, 0);
        book.onNewOrder(BID, P21, 300, ORDER_2);
        checkTopOfBook(book, P21, P0);
        checkSizeForPriceLevel(book, P21, 400, 0);
        checkBookDepth(book, 1, 0);
        book.onNewOrder(ASK, P22, 700, ORDER_3);
        checkTopOfBook(book, P21, P22);
        checkSizeForPriceLevel(book, P21, 400, 0);
        checkSizeForPriceLevel(book, P22, 0, 700);
        checkBookDepth(book, 1, 1);
        book.onReplaceOrder(P22, 150, ORDER_1);
        checkTopOfBook(book, P22, P22);
        checkSizeForPriceLevel(book, P21, 300, 0);
        checkSizeForPriceLevel(book, P22, 150, 700);
    }

    @Test
    public void loadTest() {
        int nbOrders = 500000;
        newOrderloadTest(nbOrders, 1, 10, 11);
        newOrderloadTest(nbOrders, 1, 10, 12);
        newOrderloadTest(nbOrders, 1, 10, 14);
        newOrderloadTest(nbOrders, 1, 10, 30);
        newOrderloadTest(nbOrders, 1, 10, 60);
        newOrderloadTest(nbOrders, 4, 10, 60);
        newOrderloadTest(nbOrders, 8, 10, 60);
    }


    private void newOrderloadTest(int nbOrders, int nbThreads, double minPrice, double maxPrice) {
        System.out.println("------------------------------------------------------------------------");
        System.out.println("NewOrderLoadTest: nbOrders = " + nbOrders + " nbThreads = " + nbThreads);
        System.out.println("minPrice = " + minPrice + " maxPrice = " + maxPrice);
        System.out.println("------------------------------------------------------------------------");
        Level2View book = createOrderBook();
        int priceScale = 4;
        int minQty = 100;
        int maxQty = 200;

        BigDecimal bestBid = null;
        BigDecimal bestAsk = null;

        Map<BigDecimal, Long> bids = new HashMap<>();
        Map<BigDecimal, Long> asks = new HashMap<>();

        Random r = new Random(SEED);
        long orderId = 0;

        List<Runnable> actionQueue = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(nbOrders);
        for (int i = 1; i <= nbOrders; i++) {
            BigDecimal price = BigDecimal.valueOf(r.nextDouble() * (maxPrice - minPrice) + minPrice).setScale(priceScale, RoundingMode.UP);
            long quantity = r.nextInt(maxQty) + minQty;
            Level2View.Side side = r.nextBoolean() ? BID : ASK;
            if (side == BID) {
                Long currentQuantity = bids.get(price);
                if (currentQuantity == null) currentQuantity = 0L;
                bids.put(price, currentQuantity + quantity);
                if (bestBid == null || price.compareTo(bestBid) > 0) { // Mind the comparison sign!
                    bestBid = price;
                }
            } else {
                Long currentQuantity = asks.get(price);
                if (currentQuantity == null) currentQuantity = 0L;
                asks.put(price, currentQuantity + quantity);
                if (bestAsk == null || price.compareTo(bestAsk) < 0) { // Mind the comparison sign!
                    bestAsk = price;
                }
            }
            actionQueue.add(new NewOrderAction(book, side, price, quantity, orderId++, countDownLatch));
        }

        System.out.println("Action queue created: " + actionQueue.size() + " actions");
        System.out.println("bids depth: " + bids.size() + ", best bid = " + bestBid);
        System.out.println("asks depth: " + asks.size() + ", best ask = " + bestAsk);

        System.out.println("Submitting actions...");
        ExecutorService executor = Executors.newFixedThreadPool(nbThreads);
        for (Runnable action : actionQueue) {
            executor.submit(action);
        }
        System.out.println("All actions submitted");
        try {
            long start = System.currentTimeMillis();
            countDownLatch.await();
            long end = System.currentTimeMillis();
            System.out.println("All actions completed in " + (end - start) + " ms");

            System.out.println("Checking book state: depth, top of book, size for each price level...");
            start = System.currentTimeMillis();
            checkBookDepth(book, bids.size(), asks.size());
            checkTopOfBook(book, bestBid, bestAsk);
            for (BigDecimal price : bids.keySet()) {
                Assert.assertEquals(bids.get(price), Long.valueOf(book.getSizeForPriceLevel(BID, price)));
            }
            for (BigDecimal price : asks.keySet()) {
                Assert.assertEquals(asks.get(price), Long.valueOf(book.getSizeForPriceLevel(ASK, price)));
            }
            end = System.currentTimeMillis();
            System.out.println("Book state checks completed in " + (end - start) + " ms");

        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }


    }


    private Level2View createOrderBook() {
        return new OrderBook3();
    }

    private void checkBookDepth(Level2View orderBook, long expectedBidDepth, long expectedAskDepth) {
        Assert.assertEquals(expectedBidDepth, orderBook.getBookDepth(BID));
        Assert.assertEquals(expectedAskDepth, orderBook.getBookDepth(ASK));
    }

    private void checkTopOfBook(Level2View orderBook, BigDecimal expectedBidPrice, BigDecimal expectedAskPrice) {
        Assert.assertTrue(expectedBidPrice.compareTo(orderBook.getTopOfBook(BID)) == 0);
        Assert.assertTrue(expectedAskPrice.compareTo(orderBook.getTopOfBook(ASK)) == 0);

    }

    private void checkSizeForPriceLevel(Level2View orderBook, BigDecimal priceLevel, long expectedBidSize, long expectedAskSize) {
        Assert.assertEquals(expectedBidSize, orderBook.getSizeForPriceLevel(BID, priceLevel));
        Assert.assertEquals(expectedAskSize, orderBook.getSizeForPriceLevel(ASK, priceLevel));
    }

    private class NewOrderAction implements Runnable {

        private final Level2View book;
        private final Level2View.Side side;
        private final BigDecimal price;
        private final long quantity;
        private final long orderId;
        private final CountDownLatch countDownLatch;

        public NewOrderAction(Level2View book,
                              Level2View.Side side,
                              BigDecimal price,
                              long quantity,
                              long orderId,
                              CountDownLatch countDownLatch) {
            this.book = book;
            this.side = side;
            this.price = price;
            this.quantity = quantity;
            this.orderId = orderId;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            book.onNewOrder(side, price, quantity, orderId);
            if (countDownLatch != null) countDownLatch.countDown();
        }
    }
}
