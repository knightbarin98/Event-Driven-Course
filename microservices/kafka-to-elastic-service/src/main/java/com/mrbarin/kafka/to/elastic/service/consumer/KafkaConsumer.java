package com.mrbarin.kafka.to.elastic.service.consumer;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;
import java.util.List;

public interface KafkaConsumer <K extends Serializable, V extends SpecificRecordBase> {
    public void receive(List<K> messages, List<Integer> keys, List<Integer> partitions, List<Long> offsets);
}
