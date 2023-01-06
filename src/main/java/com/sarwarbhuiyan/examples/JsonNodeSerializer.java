package com.sarwarbhuiyan.examples;

import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

@RegisterForReflection
class JsonNodeSerializer implements Serializer<JsonNode> {
  private final static ObjectMapper objectMapper = new ObjectMapper().registerModule(new AfterburnerModule());
  private final static ObjectWriter objectWriter = objectMapper.writerFor(JsonNode.class);
  
  public JsonNodeSerializer() {
    
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  public byte[] serialize(String topic, Headers headers, JsonNode data) {
      if (data == null) {
          return null;
      } else {
          try {
              return objectWriter.writeValueAsBytes(data);
          } catch (JsonProcessingException e) {
              throw new SerializationException();
          }
      }
  }

      public byte[] serialize(String topic, JsonNode data) {
          if (data == null) {
              return null;
          } else {
              try {
                  return objectWriter.writeValueAsBytes(data);
              } catch (JsonProcessingException e) {
                  throw new SerializationException();
              }
          }
  }
  public void close() {
  }
}
