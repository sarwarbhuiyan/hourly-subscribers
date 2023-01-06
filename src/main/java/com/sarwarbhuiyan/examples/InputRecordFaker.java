package com.sarwarbhuiyan.examples;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//@RegisterForReflection
//@QuarkusMain(name="fake-record-producer")
public class InputRecordFaker  {


  public static void main(String[] args) {
    final Properties envProps = new Properties();
    FileInputStream input;
    try {
      input = new FileInputStream(args[0]);
      long maxRecords = 3600000000L;

      envProps.load(input);
      input.close();

      String topic = envProps.getProperty("input.topic.name");

      envProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

      envProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RawEventSerializer.class);
      envProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "800000");
      envProps.put(ProducerConfig.LINGER_MS_CONFIG, "10");

      Producer<String, RawEvent> producer  = new KafkaProducer<String, RawEvent>(envProps);
      Faker faker = new Faker();

      for(long i=0; i<maxRecords; i++) {
        RawEvent event = new RawEvent();
        event.setBlocked(faker.bool().bool());
        event.setDstAddr(faker.internet().ipV4Address());
        event.setDstPort(Long.toString(faker.number().randomNumber(4, false)));
        event.setSrcAddr(faker.internet().ipV4Address());
        event.setSrcPort(Long.toString(faker.number().randomNumber(4, false)));
        event.setHost(faker.internet().domainName());
        event.setSubscriber(faker.internet().emailAddress());
        event.setTime(faker.date().past(10, TimeUnit.SECONDS).getTime()/1000.0f);
        event.setHttps(faker.bool().bool());

        producer.send(new ProducerRecord<String, RawEvent>(topic, event));

      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    
    
  }
}
