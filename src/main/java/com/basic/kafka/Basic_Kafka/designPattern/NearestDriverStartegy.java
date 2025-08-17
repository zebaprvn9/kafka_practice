package com.basic.kafka.Basic_Kafka.designPattern;

import java.util.Comparator;
import java.util.List;

public class NearestDriverStartegy implements MatchingStartegy {
    @Override
    public Driver match(RideRequest rquest, List<Driver> nearByDriver) {
        return nearByDriver.stream().min(Comparator.comparingDouble(d-> Haversine.distanceKm(
                rquest.lat, rquest.lon, d.lat, d.lon

        ))).orElse(null);
    }
}
