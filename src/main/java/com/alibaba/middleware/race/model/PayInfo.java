package com.alibaba.middleware.race.model;

/**
 * Created by kevin on 16-7-8.
 */
public class PayInfo {
    private long timestamp;
    private double price;

    public PayInfo(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
