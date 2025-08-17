package com.basic.kafka.Basic_Kafka.designPattern;

import java.util.Comparator;
import java.util.List;

public class FastestETAStartegy implements MatchingStartegy {
    /**
     * @param rquest
     * @param nearByDriver
     * @return
     */
    @Override
    public Driver match(RideRequest rquest, List<Driver> nearByDriver) {
        return nearByDriver.stream().min(Comparator.comparingDouble(d-> d.etaSeconds)).orElse(null);
    }
}
