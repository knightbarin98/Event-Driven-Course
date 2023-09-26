package com.mrbarin.kafka.to.elastic.service.consumer.impl;

import com.mrbarin.demo.config.KafkaConfigData;
import com.mrbarin.kafka.admin.client.KafkaAdminClient;
import com.mrbarin.kafka.avro.model.TwitterAvroModel;
import com.mrbarin.kafka.to.elastic.service.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.List;

public class TwitterKafkaConsumer implements KafkaConsumer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaConsumer.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaConfigData kafkaConfigData;

    public TwitterKafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                                KafkaAdminClient kafkaAdminClient, KafkaConfigData kafkaConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
    }

    @Override
    @KafkaListener(id = "twitterTopicListener", topics="${kafka-config.topic-name}")
    public void receive(
            @Payload List<Long> messages,
            @Header(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
        LOG.info("{} number of message received with keys {}, partitions {} and offsets {} " +
                "sending it to elastic: Thread id {}",
                messages.size(),
                keys.toString(),
                partitions.toString(),
                offsets.toString(),
                Thread.currentThread().getId()
                );
    }
}
