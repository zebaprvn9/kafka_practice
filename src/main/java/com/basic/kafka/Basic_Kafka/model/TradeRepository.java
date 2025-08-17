package com.basic.kafka.Basic_Kafka.model;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface TradeRepository extends MongoRepository<TradeData, String> {

    List<TradeData> findByStockSymbol(String stockSymbol);
    List<TradeData> findByTimestampAfter(LocalDateTime timestamp);
    List<TradeData> findByStockSymbolAndTimestampAfter(String stockSymbol, LocalDateTime timestamp);


}
