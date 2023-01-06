package com.sarwarbhuiyan.examples;

import java.io.IOException;
import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

@RegisterForReflection
public class JsonNodeDeserializer implements Deserializer<JsonNode> {
  private final static ObjectMapper objectMapper = new ObjectMapper().registerModule(new AfterburnerModule());
  private final static ObjectReader objectReader = objectMapper.readerFor(JsonNode.class);
  
  public JsonNodeDeserializer() {
    
  }

  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
      // nothing to configure
  }

  @Override
  public JsonNode deserialize(final String topic, byte[] bytes) {
      if (bytes == null)
          return null;

      try {
          return objectReader.readTree(bytes);
      } catch (final IOException e) {
          throw new SerializationException(e);
      }
  }

  @Override
  public void close() {
  }
}
