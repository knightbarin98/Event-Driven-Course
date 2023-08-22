package com.mrbarin.demo.config.twitter.to.kafka.service.runner.impl;

import com.mrbarin.demo.config.TwitterToKafkaServiceConfigData;
import com.mrbarin.demo.config.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.mrbarin.demo.config.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;
@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener listener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner (TwitterToKafkaServiceConfigData configData, TwitterKafkaStatusListener listener){
        this.configData = configData;
        this.listener = listener;
    }
    @Override
    public void start() throws TwitterException {
        twitterStream = TwitterStreamFactory.getSingleton();
        twitterStream.addListener(listener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream != null){
            LOG.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter(){
        String [] keywords = configData.getTwitterKeyWords().toArray(new String[0]);
        FilterQuery query = new FilterQuery(keywords);
        twitterStream.filter(query);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}
