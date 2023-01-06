package com.sarwarbhuiyan.examples;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.JsonNode;

@RegisterForReflection
public class JsonSerde implements Serde<JsonNode> {


  @Override
  public void configure(java.util.Map<java.lang.String, ?> configs, boolean isKey) {
  }

  @Override
  public Serializer<JsonNode> serializer() {
      return new JsonNodeSerializer();
  }

  @Override
  public Deserializer<JsonNode> deserializer() {
     return new JsonNodeDeserializer();
  }

  public JsonSerde() {
  }
}
