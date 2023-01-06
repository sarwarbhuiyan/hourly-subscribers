package com.sarwarbhuiyan.examples;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.rocksdb.DBOptions;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

@RegisterForReflection(targets = { LogAndContinueExceptionHandler.class, DBOptions.class, ZstdOutputStreamNoFinalizer.class} )
public class QuarkusKafkaStreamsConfiguration {


}
