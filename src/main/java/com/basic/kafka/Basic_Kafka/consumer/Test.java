package com.basic.kafka.Basic_Kafka.consumer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Test {


/**
 * SOLID
 * S-> one class should hold only
 *
 *
 *
 *
 */



    /**
     *1. create a random number
     *2. 2 thread one print even and other print odd.
     *3. create a linked hash map from scratch using array.
     *4. stock aggregator, which read stock value from a stream
     *   stock name, stock price and stock quantity
     * @param args
     */
    public static void main(String[] args) {
        Stock s1 = new Stock("s1", new BigDecimal(200), 10);
        Stock s2 = new Stock("s2", new BigDecimal(300), 20);
        HashMap<String, BigDecimal> stockResult = new HashMap<>();

        List<Stock> stocks = new ArrayList<>();
        stocks.add(s1);
        stocks.add(s2);


        for(Stock stock : stocks) {
            if(stockResult.containsKey(stock.getStockName())) {
                stockResult.put(stock.getStockName(),
                        stockResult.get(stock.stockName).add(stock.calculatePrice(stock.getStockPrice(), stock.getQuantity())));
            }
            stockResult.put(stock.getStockName(), stock.calculatePrice(stock.getStockPrice(), stock.getQuantity()));
        }

        System.out.println(stockResult);

    }
}
