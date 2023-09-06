package com.mrbarin.common.config;

import com.mrbarin.demo.config.RetryConfigData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    private final RetryConfigData configData;

    public RetryConfig(RetryConfigData configData){
        this.configData = configData;
    }

    @Bean
    public RetryTemplate retryTemplate(){
        RetryTemplate retryTemplate = new RetryTemplate();

        //ExponentialBackOffPolicy Increase wait time for each retyr attempt
        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(configData.getInitialIntervalMs());
        exponentialBackOffPolicy.setMaxInterval(configData.getMaxIntervalMs());
        exponentialBackOffPolicy.setMultiplier(configData.getMultiplier());

        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(configData.getMaxAttempts());

        retryTemplate.setRetryPolicy(simpleRetryPolicy);


        return retryTemplate;
    }
}
