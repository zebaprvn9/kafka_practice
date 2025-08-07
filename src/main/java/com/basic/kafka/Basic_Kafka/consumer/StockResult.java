package com.basic.kafka.Basic_Kafka.consumer;

import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;
@Data
@ToString
public class StockResult {

    private String stockName;
    private BigDecimal stockValue;
}
