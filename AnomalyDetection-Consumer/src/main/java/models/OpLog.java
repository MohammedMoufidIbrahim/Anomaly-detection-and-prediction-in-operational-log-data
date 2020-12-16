package models;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import com.google.gson.JsonObject;

public final class OpLog{
	private final String responseTime;
	private final String eventStartMs;
	private final String eventStartEpoc;
	private final int year;
	private final int month;
	private final int day;
	
    private OpLog(final Builder builder) {
        this.responseTime = builder.responseTime;
        this.eventStartMs = builder.eventStartMs;
        this.eventStartEpoc = builder.eventStartEpoc;
        this.year = builder.year;
        this.month = builder.month;
        this.day = builder.day;
    }
    
    private OpLog(String responseTime, String eventStartMs, String eventStartEpoc, int year, int month, int day) {
        this.responseTime = responseTime;
        this.eventStartMs = eventStartMs;
        this.eventStartEpoc = eventStartEpoc;
        this.year = year;
        this.month = month;
        this.day = day;
    }
    
    public OpLog createNewOpLogFromJson(JsonObject json) {
        String responseTime = json.get("responseTime").getAsString();
        String eventStartMs = json.get("eventStartMs").getAsString();
        String eventStartEpoc = json.get("eventStartEpoc").getAsString();
        int year = Integer.parseInt(json.get("year").getAsString());
        int month = Integer.parseInt(json.get("month").getAsString());
        int day = Integer.parseInt(json.get("day").getAsString());
        return new OpLog(responseTime, eventStartMs, eventStartEpoc, year, month, day);
    }

    public String getResponseTime() { return this.responseTime; }
    public String getEventStartMs() { return this.eventStartMs; }
    public String getEventStartEpoc() { return this.eventStartEpoc; }
    public int getYear() { return this.year; }
    public int getMonth() { return this.month; }
    public int getDay() { return this.day; }
    
    @Override
    public int hashCode() {
        return Objects.hash(responseTime, eventStartMs, eventStartEpoc, year, month, day);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OpLog other = (OpLog) obj;
        return Objects.equals(this.responseTime, other.responseTime)
                && Objects.equals(this.eventStartMs, other.eventStartMs)
                && Objects.equals(this.eventStartEpoc, other.eventStartEpoc)
                && Objects.equals(this.year, other.year)
                && Objects.equals(this.month, other.month)
                && Objects.equals(this.day, other.day);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("responseTime", responseTime)
                .add("eventStartMs", eventStartMs)
                .add("eventStartEpoc", eventStartEpoc)
                .add("year", year)
                .add("month", month)
                .add("day", day)
                .toString();
    }

    public static class Builder{       
    	private String responseTime;
    	private String eventStartMs;
    	private String eventStartEpoc;
    	private int year;
    	private int month;
    	private int day;

        public Builder responseTime(final String responseTime) {
            this.responseTime = responseTime;
            return this;
        }

        public Builder eventStartMs(final String eventStartMs) {
            this.eventStartMs = eventStartMs;
            return this;
        }

        public Builder eventStartEpoc(final String eventStartEpoc) {
            this.eventStartEpoc = eventStartEpoc;
            return this;
        }

        public Builder year(final int year) {
            this.year = year;
            return this;
        }
        
        public Builder month(final int month) {
            this.month = month;
            return this;
        }
        
        public Builder day(final int day) {
            this.day = day;
            return this;
        }

        public OpLog build() {
            return new OpLog(this);
        }
    }
}