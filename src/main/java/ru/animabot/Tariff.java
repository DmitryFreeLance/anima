package ru.animabot;

public class Tariff {
    private int id;
    private String name;
    private String price;
    private int periodDays;
    private String payUrl;

    public Tariff() {}
    public Tariff(int id, String name, String price, int periodDays, String payUrl) {
        this.id = id; this.name = name; this.price = price; this.periodDays = periodDays; this.payUrl = payUrl;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }
    public int getPeriodDays() { return periodDays; }
    public void setPeriodDays(int periodDays) { this.periodDays = periodDays; }
    public String getPayUrl() { return payUrl; }
    public void setPayUrl(String payUrl) { this.payUrl = payUrl; }
}