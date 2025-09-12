package com.basic.kafka.Basic_Kafka.service;

import com.basic.kafka.Basic_Kafka.model.TradeData;
import com.basic.kafka.Basic_Kafka.model.TradeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class TradeService {

    private final TradeRepository tradeRepository;

    public TradeService(TradeRepository tradeRepository) {
        this.tradeRepository = tradeRepository;
    }

    @Autowired
    TradeRepository tradeRepository1;


    /**
     * Kafka consumer will call this for every message
     */
    public void consumeData(TradeData trade) {
        tradeRepository.save(trade);
    }

    /**
     * Trigger every 5 minutes
     * Reads trades from DB asynchronously and generates report for matching stocks
     */
    @Scheduled(fixedRate = 300_000)
    public void processData() {
        double targetPrice = 100.50;
        LocalDateTime fiveMinutesAgo = LocalDateTime.now().minus(5, ChronoUnit.MINUTES);

        fetchRecentTradesAsync(fiveMinutesAgo)
                .thenAccept(recentTrades -> {
                    // Group trades by stock symbol
                    Map<String, List<TradeData>> groupedTrades = recentTrades.stream()
                            .collect(Collectors.groupingBy(TradeData::getStockSymbol));

                    // Prepare a list of matching trades
                    List<TradeData> matchedTrades = new ArrayList<>();

                    groupedTrades.forEach((symbol, trades) -> {
                        TradeData latestTrade = trades.stream()
                                .max(Comparator.comparing(TradeData::getTimestamp))
                                .orElse(null);

                        if (groupedTrades != null &&
                                Double.compare(latestTrade.getLastPrice(), targetPrice) == 0) {
                            matchedTrades.add(latestTrade);
                        }
                    });

                    if (!matchedTrades.isEmpty()) {
                        generateReportAsync(matchedTrades);
                    }
                });
    }


    @Async
    public CompletableFuture<List<TradeData>> fetchRecentTradesAsync(LocalDateTime fromTime) {
        return CompletableFuture.supplyAsync(() -> tradeRepository.findByTimestampAfter(fromTime));
    }


    @Async
    public void generateReportAsync(List<TradeData> trades) {
        CompletableFuture.runAsync(() -> {
            System.out.println("\n===== Price Match Report =====");
            trades.forEach(trade -> {
                System.out.println("Stock Symbol : " + trade.getStockSymbol());
                System.out.println("Last Price   : " + trade.getLastPrice());
                System.out.println("Volume       : " + trade.getVolume());
                System.out.println("Time         : " + trade.getTimestamp());
                System.out.println("-------------------------------");
            });
        });
    }
}
