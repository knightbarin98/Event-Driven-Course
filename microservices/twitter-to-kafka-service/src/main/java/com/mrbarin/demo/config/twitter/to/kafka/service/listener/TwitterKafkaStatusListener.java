package com.mrbarin.demo.config.twitter.to.kafka.service.listener;

import com.mrbarin.demo.config.KafkaConfigData;
import com.mrbarin.demo.config.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import com.mrbarin.kafka.avro.model.TwitterAvroModel;
import com.mrbarin.kafka.producer.service.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer transformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData,
                                      KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                      TwitterStatusToAvroTransformer transformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.transformer = transformer;
    }

    @Override
    public void onStatus(Status status) {
        LOG.info("Received status text {} sending to kafka topic {}", status.getText(), kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = transformer.getTwitterAvroModelFromStatus(status);
        //Kafka partition kety: set the target partition for message
        //In this case is creating a partition for specific user
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getId(), twitterAvroModel);
    }
}
