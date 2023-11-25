package com.mrbarin.kafka.to.elastic.service.consumer.impl;

import com.mrbarin.demo.config.KafkaConfigData;
import com.mrbarin.demo.config.KafkaConsumerConfigData;
import com.mrbarin.elastic.index.client.service.ElasticIndexClient;
import com.mrbarin.elastic.model.index.impl.TwitterIndexModel;
import com.mrbarin.kafka.admin.client.KafkaAdminClient;
import com.mrbarin.kafka.avro.model.TwitterAvroModel;
import com.mrbarin.kafka.to.elastic.service.consumer.KafkaConsumer;
import com.mrbarin.kafka.to.elastic.service.transformer.AvroToElasticModelTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class TwitterKafkaConsumer implements KafkaConsumer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaConsumer.class);

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaConfigData kafkaConfigData;

    private final AvroToElasticModelTransformer avroToElasticModelTransformer;

    private final ElasticIndexClient<TwitterIndexModel> indexModelElasticIndexClient;

    private final KafkaConsumerConfigData kafkaConsumerConfigData;

    public TwitterKafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                                KafkaAdminClient kafkaAdminClient,
                                KafkaConfigData kafkaConfigData,
                                AvroToElasticModelTransformer avroToElasticModelTransformer,
                                ElasticIndexClient<TwitterIndexModel> indexModelElasticIndexClient,
                                KafkaConsumerConfigData kafkaConsumerConfigData) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConfigData = kafkaConfigData;
        this.avroToElasticModelTransformer = avroToElasticModelTransformer;
        this.indexModelElasticIndexClient = indexModelElasticIndexClient;
        this.kafkaConsumerConfigData = kafkaConsumerConfigData;
    }


    @EventListener
    public void onAppStarted(ApplicationStartedEvent event) {
        kafkaAdminClient.checkTopicsCreated();
        LOG.info("Topics with name {} is ready for operations!", kafkaConfigData.getTopicNamesToCreate().toArray());
        Objects.requireNonNull(
                kafkaListenerEndpointRegistry.getListenerContainer(kafkaConsumerConfigData.getConsumerGroupId())
        ).start();
    }

    @Override
    @KafkaListener(id = "${kafka-consumer-config.consumer-group-id}", topics = "${kafka-config.topic-name}")
    public void receive(
            @Payload List<TwitterAvroModel> messages,
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
        List<TwitterIndexModel> twitterIndexModels = avroToElasticModelTransformer.getElasticModels(messages);
        List<String> ids = indexModelElasticIndexClient.save(twitterIndexModels);
        LOG.info("Documents send to elasticsearch with ids {}", ids.toArray());
    }
}
