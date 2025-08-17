package com.basic.kafka.Basic_Kafka.designPattern;

// File: geo/Geohash.java
// Minimal geohash encoder (Base32); precision 5–7 is typical for city blocks.
public class Geohash {
    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    public static String encode(double lat, double lon, int precision) {
        double[] latRange = {-90.0, 90.0};
        double[] lonRange = {-180.0, 180.0};
        boolean isEven = true;
        int bit = 0, ch = 0;
        StringBuilder geohash = new StringBuilder();

        while (geohash.length() < precision) {
            double mid;
            if (isEven) {
                mid = (lonRange[0] + lonRange[1]) / 2D;
                if (lon > mid) { ch |= (1 << (4 - bit)); lonRange[0] = mid; }
                else           { lonRange[1] = mid; }
            } else {
                mid = (latRange[0] + latRange[1]) / 2D;
                if (lat > mid) { ch |= (1 << (4 - bit)); latRange[0] = mid; }
                else           { latRange[1] = mid; }
            }
            isEven = !isEven;
            if (bit < 4) bit++;
            else {
                geohash.append(BASE32.charAt(ch));
                bit = 0; ch = 0;
            }
        }
        return geohash.toString();
    }

    // Return the 8 neighbors + self prefix for a simple adjacent search (prefix-only approach).
    // For simplicity we compute neighbors by sampling small deltas around center and encoding.
    public static java.util.Set<String> neighborhood(double lat, double lon, int precision) {
        double d = 0.002; // ~200m step near equator; adjust by precision if desired
        java.util.Set<String> cells = new java.util.HashSet<>();
        for (int i=-1; i<=1; i++) {
            for (int j=-1; j<=1; j++) {
                double la = lat + i*d;
                double lo = lon + j*d;
                cells.add(encode(la, lo, precision));
            }
        }
        return cells;
    }
}
