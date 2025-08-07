package com.basic.kafka.Basic_Kafka.consumer;

import lombok.Data;

import java.math.BigDecimal;

@Data
class Stock {

        String stockName;
        BigDecimal stockPrice;
        Integer quantity;
        public Stock(String stockName, BigDecimal stockPrice, Integer quantity) {
            this.stockName = stockName;
            this.stockPrice = stockPrice;
            this.quantity = quantity;
        }

        public BigDecimal calculatePrice(BigDecimal stockPrice, Integer quantity) {
            return stockPrice.multiply(new BigDecimal(quantity));
        }
    }