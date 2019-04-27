package com.mf.orderbook;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import static com.mf.orderbook.Level2View.Side.*;

public class OrderBookTest {

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
        Level2View book = new OrderBook();
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
        Level2View book = new OrderBook();
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
    public void getSizeForPriceLevelTest(){
        Level2View book = new OrderBook();
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
    public void onNewOrderTest(){
        Level2View book = new OrderBook();
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
    }

    @Test
    public void onCancelOrderTest(){

    }

    @Test
    public void onReplaceOrderTest(){

    }

    @Test
    public void onTradeTest(){

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

}
