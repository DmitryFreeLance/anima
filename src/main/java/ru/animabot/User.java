package ru.animabot;

import java.util.Date;

public class User {
    private long userId;
    private Date lastViewedTariffs;

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }
    public Date getLastViewedTariffs() { return lastViewedTariffs; }
    public void setLastViewedTariffs(Date lastViewedTariffs) { this.lastViewedTariffs = lastViewedTariffs; }
}
