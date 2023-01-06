package com.sarwarbhuiyan.examples;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregateEvent {
  
  @JsonProperty("subscriber")
  String subscriber;
  
  @JsonProperty("host")
  String host;
  
  @JsonProperty("firstEventTimestamp")
  long firstEventTimestamp;
  
  @JsonProperty("count")
  long count;
  
  @JsonProperty("visitedMinutesBitfield")
  long visitedMinutesBitfield;
  
  public AggregateEvent() {}
  
  public AggregateEvent(String subscriber, String host, long firstEventTimestamp, long count,
      long visitedMinutesBitfield) {
    super();
    this.subscriber = subscriber;
    this.host = host;
    this.firstEventTimestamp = firstEventTimestamp;
    this.count = count;
    this.visitedMinutesBitfield = visitedMinutesBitfield;
  }

  public String getSubscriber() {
    return subscriber;
  }

  public void setSubscriber(String subscriber) {
    this.subscriber = subscriber;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public long getFirstEventTimestamp() {
    return firstEventTimestamp;
  }

  public void setFirstEventTimestamp(long firstEventTimestamp) {
    this.firstEventTimestamp = firstEventTimestamp;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getVisitedMinutesBitfield() {
    return visitedMinutesBitfield;
  }

  public void setVisitedMinutesBitfield(long visitedMinutesBitfield) {
    this.visitedMinutesBitfield = visitedMinutesBitfield;
  }

  @Override
  public String toString() {
    return "AggregateObject [subscriber=" + subscriber + ", host=" + host + ", firstEventTimestamp="
        + firstEventTimestamp + ", count=" + count + ", visitedMinutesBitfield="
        + visitedMinutesBitfield + "]";
  }
  
  
}
