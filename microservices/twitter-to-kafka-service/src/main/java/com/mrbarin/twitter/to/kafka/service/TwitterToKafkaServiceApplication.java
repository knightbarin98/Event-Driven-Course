package com.mrbarin.twitter.to.kafka.service;

import com.mrbarin.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.mrbarin.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

//import javax.annotation.PostConstruct;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    public static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    //@Autowired
    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final StreamRunner streamRunner;

    //Prefer constructor injection
    //Favors inmutability & Forces object creation with the inject object & No reflection & no @Autowired
    //Thread safe
    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.streamRunner = streamRunner;
    }

    //Since it would not be consume by clients, we need to find a way to trigger the reading logic (Twitter)
    //We need an application general initialization job
    //Initialization logic
    //@PostConstruct vs ApplicationListener vs CommandLineRunner vs @EventListener
    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        LOG.info("Application initializing ...");
        LOG.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeyWords().toArray(new String[]{})));
        LOG.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
