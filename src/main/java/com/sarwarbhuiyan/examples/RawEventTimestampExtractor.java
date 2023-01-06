package com.sarwarbhuiyan.examples;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import com.fasterxml.jackson.databind.JsonNode;
@RegisterForReflection
public class RawEventTimestampExtractor implements TimestampExtractor {

  public RawEventTimestampExtractor() {}

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    JsonNode event = (JsonNode)record.value();
    if(event.get("time")!=null)
      return (long)(event.get("time").asLong()*1000);
    return  record.timestamp();
  }

}
