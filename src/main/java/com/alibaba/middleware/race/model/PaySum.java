package com.alibaba.middleware.race.model;

/**
 * Created by kevin on 16-7-3.
 */
public class PaySum {
    private Long timestamp;
    private int platform;
    private Double total;

    public PaySum(Long timestamp, int platform, Double total) {
        this.timestamp = timestamp;
        this.platform = platform;
        this.total = total;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public int getPlatform() {
        return platform;
    }

    public void setPlatform(int platform) {
        this.platform = platform;
    }

    public Double getTotal() {
        return total;
    }

    public void setTotal(Double total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "PaySum{" +
                "timestamp=" + timestamp +
                ", platform=" + platform +
                ", total=" + total +
                '}';
    }
}
