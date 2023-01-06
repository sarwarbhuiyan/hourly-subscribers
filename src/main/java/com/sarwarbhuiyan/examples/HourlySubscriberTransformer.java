package com.sarwarbhuiyan.examples;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RegisterForReflection
public class HourlySubscriberTransformer<K, V, R> implements Transformer<K, V, R> {

  
  private static final String SUBSCRIBER_FIELD = "subscriber";
  private static final String HOST_FIELD = "host";
  private static final String FIRST_EVENT_TIMESTAMP_FIELD = "firstEventTimestamp";
  private static final String COUNT_FIELD = "count";
  private static final String VISITED_MINUTES_BITFIELD_FIELD = "visitedMinutesBitfield";
  private static final int GRACE_PERIOD_HOURS = 1;
  private static final int PUNCTUATOR_MS_CUTOFF = 60000;
  private ProcessorContext context;
  private String stateStoreName;
  private KeyValueStore<String, byte[]> stateStore;
  private String timeZone;
  private JsonNodeSerializer serializer = new JsonNodeSerializer();
  private JsonNodeDeserializer deserializer = new JsonNodeDeserializer();
  
  public HourlySubscriberTransformer(String stateStoreName, String timeZone) {
    this.stateStoreName = stateStoreName;
    this.timeZone = timeZone;
  }
  
  
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.stateStore = context.getStateStore(stateStoreName);
    
    // This acts like a "cron" job except it is called on each record going through the topology IF the time interval shown has passed but in "event time". 
    // event time is the time that the stream processor has seen in the data and can assume that the time has moved onwards.
    context.schedule(Duration.ofSeconds(10L), PunctuationType.STREAM_TIME, timestamp -> {

      // Producer record
      // key
      // timestamp - by default producer, also override in send(), broker() (LogAppendTime). 
      // headers
      // payload
      
      // we check what is the event time and extract the day number and hour number
      ZonedDateTime currentZdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of(timeZone));
      int today = currentZdt.getDayOfYear();
      int currentHour = currentZdt.getHour(); 
      System.out.println("current day hour "+today+"-"+currentHour);
      long startTimestamp = System.currentTimeMillis();
      long processed = 0;
      // we do a range lookup from beginning of the state store to where the key is ${today}-${current_hour}
      try (final KeyValueIterator<String, byte[]> all = stateStore.range(null, today+"-"+currentHour)) {
        
        while(all.hasNext()) {
          final KeyValue<String, byte[]> record = all.next();
          //String[] keyParts = record.key.split("-");
          //int day = Integer.parseInt(keyParts[0]);
          //int hour = Integer.parseInt(keyParts[1]);
          
          // we do an additional check to ensure the record's hour is not the current hour just for safety
          //System.out.println("Forwarding 'final' results for key="+record.key+" agg="+record.value);
            
          // Now we emit the Aggregate record that was there
          context.forward(record.key, record.value);
          stateStore.put(record.key, null); // using tombstone rather than delete because delete does and write
          
          processed++;
          
          
          long currentTimestamp = System.currentTimeMillis();
          if(currentTimestamp - startTimestamp > PUNCTUATOR_MS_CUTOFF) {
            System.out.println("Trapping out of punctuator having forwarded "+processed+" records");
            return; // trap out as punctuator is taking too long
          }
          // We remove the Aggregate record from the local state store as we are done with it. 
          // if 24 hours has passed, delete it. 
          //int hourDifference = (today - day)*24 + (currentHour - hour);
          
          //if(hourDifference > GRACE_PERIOD_HOURS) // this is the grace period for super late arriving data
          //  stateStore.delete(record.key);
         
          
        }
        
        System.out.println("Forwarded "+processed+" records without trapping out");
      } 
      
    });
  }

  /**
   * What is happening when each record is going through the topology. Here it decides whether to create a new aggregate object
   * or update an existing aggregate object (increment count and update the bitfield)
   */
  @Override
  public R transform(K key, V value) {
    JsonNode rawEvent = (JsonNode)value;
    if(rawEvent == null)
      System.out.println("rawEvent is null");
    if(rawEvent.get("time") == null)
      System.out.println("rawEvent.getTime is null");
    if(rawEvent == null || rawEvent.get("time") == null) {
      System.out.println("RAW EVENT "+rawEvent.toPrettyString());
      return null;

    }

    long millis = (long)(rawEvent.get("time").asLong()*1000);
    
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(timeZone));
    //long currentStreamTime = context.currentStreamTimeMs();
    int day = zdt.getDayOfYear();
    int hour = zdt.getHour();
    //int minute = zdt.getMinute();
    
    // long currentStreamTime = context.currentStreamTimeMs();
    //if(currentStreamTime - millis > 1000*60*60*GRACE_PERIOD_HOURS) // should we trap out if we receive data older than GRACE_PERIOD_HOURS hours?
    //  return null; 
    String subscriber = rawEvent.get(SUBSCRIBER_FIELD).asText();
    String host = rawEvent.get(HOST_FIELD).asText();
    
    String stateStoreKey = day+"-"+hour+"-"+subscriber+"-"+host; // we encode the state store with the subscriber-host-hour as key
    
    byte[] rawAggObj = stateStore.get(stateStoreKey);
    JsonNode aggObj = null;
    if(rawAggObj == null) {
      aggObj = JsonNodeFactory.instance.objectNode();
      ObjectNode objNode = (ObjectNode)aggObj;
      objNode.put(SUBSCRIBER_FIELD, subscriber);
      objNode.put(HOST_FIELD, host);
      objNode.put(FIRST_EVENT_TIMESTAMP_FIELD, millis);
      objNode.put(COUNT_FIELD, 1L);
      objNode.put(VISITED_MINUTES_BITFIELD_FIELD, updateBitField(0L, zdt));
      //System.out.println("New bitfield = "+aggObj.getVisitedMinutesBitfield());
    } else {
      aggObj = deserializer.deserialize(null, rawAggObj);
      ObjectNode objNode = (ObjectNode)aggObj;
      objNode.put(COUNT_FIELD, objNode.get(COUNT_FIELD).asLong()+1L);
      
      long currentBitField = aggObj.get(VISITED_MINUTES_BITFIELD_FIELD).asLong();
      long updatedBitField = updateBitField(currentBitField, zdt);
      //System.out.println("Updated bitfield currentBitField="+currentBitField+" updatedBitfield="+updatedBitField);
      objNode.put(VISITED_MINUTES_BITFIELD_FIELD, updatedBitField);
    }
    
    //System.out.println("Updating state store "+stateStoreKey+"="+aggObj.toPrettyString());
    // update state store with the latest data
    stateStore.put(stateStoreKey, serializer.serialize(null, aggObj));
      
    return null;
  }

  /**
   * Updating the existing bitfield with a new date time
   * 
   * @param currentBitField
   * @param zdt
   * @return
   */
  private long updateBitField(long currentBitField, ZonedDateTime zdt) {
    
    return currentBitField | (1L << zdt.getMinute());
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  

}
