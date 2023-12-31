package com.mrbarin.kafka.producer.service.impl;

import com.mrbarin.kafka.avro.model.TwitterAvroModel;
import com.mrbarin.kafka.producer.service.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }
    @PreDestroy
    public void close(){
        if(kafkaTemplate != null){
            LOG.info("Cloasing kafka producer!");
            kafkaTemplate.destroy();
        }
    }
    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("Sending message= '{}' to topic '{}'", message, topicName);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, message);
        addCallback(topicName, message, kafkaResultFuture);
    }

    private static void addCallback(String topicName, TwitterAvroModel message,
                                    ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                LOG.error("Error while sending message {} to topic {}", message.toString(), topicName);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
                RecordMetadata metadata = result.getRecordMetadata();
                LOG.debug("Received new metadata. TOpic {}; Partition {}; Offset {}; Timestap {}; at time {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime()
                        );
            }
        });
    }
}
