package com.basic.kafka.Basic_Kafka.designPattern;

import java.util.List;

public interface MatchingStartegy {

    public Driver match(RideRequest rquest, List<Driver> nearByDriver);
}
