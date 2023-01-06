package com.sarwarbhuiyan.examples;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RawEventSerializer<T> implements Serializer<T> {

  private ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  public RawEventSerializer() {
  }
  
  @Override
  public byte[] serialize(String topic, T data) {
    if(data == null) return null;
    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (Exception e) {
      throw new SerializationException("Error serializing JSON message", e);
    }
  }
  
}
