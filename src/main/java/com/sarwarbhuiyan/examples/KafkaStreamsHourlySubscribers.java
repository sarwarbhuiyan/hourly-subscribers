package com.sarwarbhuiyan.examples;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@QuarkusMain
//@RegisterForReflection
public class KafkaStreamsHourlySubscribers implements QuarkusApplication {

  private static final String HOURLY_SUBSCRIBERS_STATE_STORE = "hourly-subscribers-state-store";

  /**
   * 
   * 
   * @param envProp
   * @return
   */
  public Topology buildTopology(Properties envProp) {
    final StreamsBuilder builder = new StreamsBuilder();
    
    String inputTopicForStream = envProp.getProperty("input.topic.name");
    String outputTopic = envProp.getProperty("output.topic.name");
    
    KStream<String, JsonNode> eventsStream = builder.stream(inputTopicForStream, Consumed.with(Serdes.String(), new JsonSerde()).withTimestampExtractor(new RawEventTimestampExtractor()))
                                                 //.filter((k,v ) -> true)  // TODO Host Filtering 
                                                 .selectKey((k,v) -> v.get("subscriber").asText()+"-"+v.get("host").asText()); // repartition by subscriber or host;
                                                 //.repartition();   
    
    final StoreBuilder<KeyValueStore<String, byte[]>> storeBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(HOURLY_SUBSCRIBERS_STATE_STORE), Serdes.String(), Serdes.ByteArray());
    
    builder.addStateStore(storeBuilder);
    
    KStream<String, byte[]> aggregateEvents = eventsStream
        .transform(() -> new HourlySubscriberTransformer<String, JsonNode, KeyValue<String, byte[]>>(HOURLY_SUBSCRIBERS_STATE_STORE, "Europe/London"), 
            HOURLY_SUBSCRIBERS_STATE_STORE);
    
    // write out aggregate events to output topic
    aggregateEvents.to(outputTopic, Produced.with(Serdes.String(), Serdes.ByteArray()));

    
    //GlobalKTable<String, AggregateEvent> aggEvents = builder.globalTable(outputTopic, Consumed.with(Serdes.String(), aggregateEventSerde), Materialized.<String, AggregateEvent, KeyValueStore<Bytes,byte[]>>as("globalAggEventStore"));
    
    return builder.build();
  }


 
  
  public Properties getStreamProps(Properties envProp) {
    final Properties streamsConfiguration = new Properties();
    //merge from properties file
    streamsConfiguration.putAll(envProp);
    
    
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, envProp.get("application.id"));
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProp.get("bootstrap.servers"));
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, RawEventTimestampExtractor.class);

    //streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 20000);
    
    
    // These two settings are only required in this contrived example so that the 
    // streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return streamsConfiguration;
  }

  public void createTopics(final Properties envProps) {
    final Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
    for(Entry<Object, Object> e: envProps.entrySet()) {
      config.put((String)e.getKey(), e.getValue());
    }

    try (final AdminClient client = AdminClient.create(config)) {

      final List<NewTopic> topics = new ArrayList<>();

      topics.add(new NewTopic(envProps.getProperty("input.topic.name"),
          Integer.parseInt(envProps.getProperty("input.topic.partitions")),
          Short.parseShort(envProps.getProperty("input.topic.replication.factor"))));

      Map<String,String> configs = new HashMap<>();
      configs.put(TopicConfig.RETENTION_MS_CONFIG, "5184000000");
      
      topics.add(new NewTopic(envProps.getProperty("output.topic.name"),
          Integer.parseInt(envProps.getProperty("output.topic.partitions")),
          Short.parseShort(envProps.getProperty("output.topic.replication.factor"))).configs(configs));

      client.createTopics(topics).all().get();
    } catch(TopicExistsException e) {
      e.printStackTrace();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (ExecutionException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  public Properties loadEnvProperties(String fileName) throws IOException {
    final Properties envProps = new Properties();
    final FileInputStream input = new FileInputStream(fileName);
    envProps.load(input);
    input.close();
    

    
    // These two settings are only required in this contrived example so that the 
    // streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return envProps;
  }

  public static void main(String[] args) throws Exception {
//
//    if (args.length < 1) {
//      throw new IllegalArgumentException(
//          "This program takes one argument: the path to an environment configuration file.");
//    }
//
//    final KafkaStreamsHourlySubscribers instance = new KafkaStreamsHourlySubscribers();
//    final Properties envProps = instance.loadEnvProperties(args[0]);
//
//    // Setup the input topic, table topic, and output topic
//    instance.createTopics(envProps);
//
//    // Normally these can be run in separate applications but for the purposes of the demo, we
//    // just run both streams instances in the same application
//
//    try (final KafkaStreams streams = new KafkaStreams(instance.buildTopology(envProps), instance.getStreamProps(envProps))) {
//     final CountDownLatch startLatch = new CountDownLatch(1);
//     // Attach shutdown handler to catch Control-C.
//     Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//         @Override
//         public void run() {
//             //streams.cleanUp();
//             streams.close(Duration.ofSeconds(5));
//             startLatch.countDown();
//         }
//     });
//     streams.setUncaughtExceptionHandler(e -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
//     // Start the topology.
//     streams.start();
//
//     try {
//       startLatch.await();
//     } catch (final InterruptedException e) {
//       Thread.currentThread().interrupt();
//       System.exit(1);
//     }
//    }
//    System.exit(0);

    Quarkus.run(KafkaStreamsHourlySubscribers.class, args);
  }

  @Override
  public int run(String... args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException(
              "This program takes one argument: the path to an environment configuration file.");
    }

    final KafkaStreamsHourlySubscribers instance = new KafkaStreamsHourlySubscribers();
    final Properties envProps = instance.loadEnvProperties(args[0]);

    // Setup the input topic, table topic, and output topic
    instance.createTopics(envProps);

    // Normally these can be run in separate applications but for the purposes of the demo, we
    // just run both streams instances in the same application

    try (final KafkaStreams streams = new KafkaStreams(instance.buildTopology(envProps), instance.getStreamProps(envProps))) {
      final CountDownLatch startLatch = new CountDownLatch(1);
      // Attach shutdown handler to catch Control-C.
      Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
        @Override
        public void run() {
          //streams.cleanUp();
          streams.close(Duration.ofSeconds(5));
          startLatch.countDown();
        }
      });
      streams.setUncaughtExceptionHandler(e -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
      // Start the topology.
      streams.start();
      Quarkus.waitForExit();

      return 0;
    }
  }
}
