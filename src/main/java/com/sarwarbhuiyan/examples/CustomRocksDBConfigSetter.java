package com.sarwarbhuiyan.examples;

import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

@RegisterForReflection
public class CustomRocksDBConfigSetter implements RocksDBConfigSetter {

  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {
    options.setCompactionStyle(CompactionStyle.LEVEL);
    options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
  }

  @Override
  public void close(String storeName, Options options) {
    // TODO Auto-generated method stub
    
  }

}
