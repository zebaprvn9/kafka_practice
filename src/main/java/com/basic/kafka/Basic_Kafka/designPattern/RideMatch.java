package com.basic.kafka.Basic_Kafka.designPattern;

// File: model/RideMatch.java
public class RideMatch {
    public final String rideId;
    public final String riderId;
    public final String driverId;
    public final double driverLat;
    public final double driverLon;
    public final double distanceKm;
    public final long matchedAt;

    public RideMatch(String rideId, String riderId, String driverId, double driverLat, double driverLon, double distanceKm, long matchedAt) {
        this.rideId = rideId;
        this.riderId = riderId;
        this.driverId = driverId;
        this.driverLat = driverLat;
        this.driverLon = driverLon;
        this.distanceKm = distanceKm;
        this.matchedAt = matchedAt;
    }

    @Override public String toString() {
        return "RideMatch{rideId=" + rideId + ", riderId=" + riderId + ", driverId=" + driverId +
               ", distanceKm=" + String.format("%.2f", distanceKm) + ", matchedAt=" + matchedAt + "}";
    }
}
