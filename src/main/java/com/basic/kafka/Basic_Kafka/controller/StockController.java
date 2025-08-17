package com.basic.kafka.Basic_Kafka.controller;

import com.basic.kafka.Basic_Kafka.model.TradeData;
import com.basic.kafka.Basic_Kafka.service.TradeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
public class StockController {

    private final TradeService stockService;

    public StockController(TradeService stockService) {
        this.stockService = stockService;
    }
//
//    @GetMapping("/stocks")
//    public CompletableFuture<List<TradeData>> getStock(@RequestParam String symbol) {
//        return stockService.getStockDataAsync(symbol);
//    }
}
