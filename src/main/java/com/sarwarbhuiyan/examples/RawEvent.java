package com.sarwarbhuiyan.examples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RawEvent {

  @JsonProperty("time")
  float time;
  
  @JsonProperty("srcAddr")
  String srcAddr;
  
  @JsonProperty("srcPort")
  String srcPort;
  
  @JsonProperty("dstAddr")
  String dstAddr;
  
  @JsonProperty("dstPort")
  String dstPort;
  
  @JsonProperty("subscriber")
  String subscriber;
  
  @JsonProperty("host")
  String host;
  
  @JsonProperty("blocked")
  boolean blocked;
  
  @JsonProperty("https")
  boolean https;

  public float getTime() {
    return time;
  }

  public void setTime(float time) {
    this.time = time;
  }

  public String getSrcAddr() {
    return srcAddr;
  }

  public void setSrcAddr(String srcAddr) {
    this.srcAddr = srcAddr;
  }

  public String getSrcPort() {
    return srcPort;
  }

  public void setSrcPort(String srcPort) {
    this.srcPort = srcPort;
  }

  public String getDstAddr() {
    return dstAddr;
  }

  public void setDstAddr(String dstAddr) {
    this.dstAddr = dstAddr;
  }

  public String getDstPort() {
    return dstPort;
  }

  public void setDstPort(String dstPort) {
    this.dstPort = dstPort;
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

  public boolean isBlocked() {
    return blocked;
  }

  public void setBlocked(boolean blocked) {
    this.blocked = blocked;
  }

  public boolean isHttps() {
    return https;
  }

  public void setHttps(boolean https) {
    this.https = https;
  }

  public RawEvent() {}

  @Override
  public String toString() {
    return "RawEvent [time=" + time + ", srcAddr=" + srcAddr + ", srcPort=" + srcPort + ", dstAddr="
        + dstAddr + ", dstPort=" + dstPort + ", subscriber=" + subscriber + ", host=" + host
        + ", blocked=" + blocked + ", https=" + https + "]";
  }
  
 

}
