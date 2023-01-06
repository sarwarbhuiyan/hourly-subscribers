package com.sarwarbhuiyan.examples;

import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SerdeUtils {

  public static <T> Serializer<T> serializer(Class clazz) {
    return new Serializer<T>() {
      private ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      
      @Override
      public byte[] serialize(String topic, T data) {
        if(data == null) return null;
        try {
          return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
          throw new SerializationException("Error serializing JSON message", e);
        }
      }

    };
  }
  
  public static <T> Deserializer<T> deserializer(Class clazz) {
    return new Deserializer<T>() {
      private ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      
      @Override
      public T deserialize(String topic, byte[] data) {
        if(data == null) return null;
        T result;
        try {
          result = (T) OBJECT_MAPPER.readValue(data, clazz);
        } catch (IOException e) {
          throw new SerializationException(e);
        }
        return result;
      }
      
    };
  }

  public static <T> Serde<T> serde(Class<T> clazz) {
        return Serdes.serdeFrom(serializer(clazz), deserializer(clazz));
  }
}
