package com.basic.kafka.Basic_Kafka.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "trade_data")
@Data
public class TradeData {

    @Id
    private String id;
    private String stockSymbol;
    private double volume;
    private double lastPrice;
    private LocalDateTime timestamp;
}
