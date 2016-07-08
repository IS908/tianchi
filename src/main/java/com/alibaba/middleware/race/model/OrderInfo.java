package com.alibaba.middleware.race.model;

/**
 * Created by kevin on 16-7-8.
 */
public class OrderInfo {
    private String paltform;
    private double price;

    public OrderInfo(String paltform, double price) {
        this.paltform = paltform;
        this.price = price;
    }

    public String getPaltform() {
        return paltform;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
