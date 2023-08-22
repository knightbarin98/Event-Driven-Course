package com.mrbarin.demo.config.twitter.to.kafka.service.runner.impl;

import com.mrbarin.demo.config.TwitterToKafkaServiceConfigData;
import com.mrbarin.demo.config.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.mrbarin.demo.config.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.mrbarin.demo.config.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[]{"Sed", "aliquet", "sed", "odio", "non", "blandit.", "Interdum", "et", "malesuada", "fames", "ac", "ante", "ipsum", "primis", "in", "faucibus.", "Vestibulum", "porta", "non", "justo", "nec", "volutpat.", "Donec", "at", "volutpat", "quam.", "Proin", "ut", "luctus", "nisi.", "Quisque", "eget", "est", "sed", "felis", "sodales", "venenatis", "et", "ac", "dui.", "Maecenas", "ultricies,", "ligula", "non", "pellentesque", "pretium,", "diam", "tortor", "ultrices", "lorem,", "ut", "fringilla", "lectus", "ex", "quis", "justo",

            "Curabitur", "tempus", "nec", "ipsum", "nec", "egestas.", "Nulla", "in", "risus", "molestie,", "rutrum", "mauris", "non,", "laoreet", "arcu.", "Ut", "posuere", "mi", "sed", "mauris", "fringilla,", "ac", "facilisis", "risus", "tincidunt.", "Phasellus", "sit", "amet", "felis", "nec", "nisi", "sollicitudin", "faucibus.", "In", "hac", "habitasse", "platea", "dictumst.", "Vivamus", "vitae", "odio", "malesuada", "libero", "luctus", "aliquam", "ac", "et", "eros.", "Mauris", "vehicula", "velit", "non", "dui", "porta,", "sit", "amet", "sodales", "nunc", "porttitor.", "Aenean", "consequat", "tortor", "eget", "tincidunt", "mattis.", "Maecenas", "mattis", "felis", "nulla,", "at", "feugiat", "sapien", "facilisis", "in.", "Donec", "laoreet", "ullamcorper", "ante.", "Donec", "aliquam", "neque", "ac", "fringilla", "ornare.", "Curabitur", "ut", "urna", "vitae", "justo", "iaculis", "accumsan", "at", "eget", "mauris.", "Quisque", "mollis", "eros", "ut", "eros", "molestie,", "vel", "vehicula", "nisi", "volutpat.", "Vestibulum", "id", "felis", "sodales", "sem", "ullamcorper", "vehicula.", "Ut", "eu", "volutpat", "ligula",

            "Nunc", "et", "sapien", "nisi.", "Fusce", "at", "gravida", "turpis,", "at", "porta", "quam.", "Donec", "in", "sapien", "ullamcorper", "nisl", "maximus", "tempus", "in", "lacinia", "neque.", "Pellentesque", "id", "ultricies", "nisl.", "Aliquam", "eu", "mollis", "mauris.", "Praesent", "pretium", "viverra", "est", "quis", "hendrerit.", "Suspendisse", "mollis", "libero", "arcu,", "eu", "pulvinar", "dui", "laoreet", "sit", "amet.", "Aenean", "auctor", "odio", "posuere", "urna", "mollis", "efficitur",

            "Integer", "tristique,", "felis", "a", "tincidunt", "bibendum,", "risus", "magna", "vulputate", "orci,", "at", "dapibus", "diam", "eros", "eget", "diam.", "Ut", "consequat", "volutpat", "est", "in", "blandit.", "Morbi", "vitae", "metus", "aliquam,", "ultricies", "felis", "non,", "vulputate", "massa.", "Duis", "efficitur", "lorem", "odio,", "ac", "rutrum", "orci", "posuere", "eget.", "Praesent", "orci", "nibh,", "sollicitudin", "a", "ex", "id,", "iaculis", "facilisis", "arcu.", "Duis", "accumsan", "velit", "ut", "magna", "lobortis,", "eget", "tincidunt", "justo", "suscipit.", "Aenean", "quis", "cursus", "massa.", "Nulla", "lobortis", "cursus", "nulla", "id", "egestas.", "Pellentesque", "malesuada", "efficitur", "purus,", "sit", "amet", "luctus", "turpis", "mattis", "nec.", "Duis", "aliquam", "in", "diam", "at", "volutpat.", "Integer", "blandit", "diam", "quis", "ex", "posuere,", "eu", "faucibus", "lorem", "pellentesque.", "Sed", "ipsum", "dolor,", "interdum", "nec", "ultrices", "sed,", "aliquam", "nec", "est.", "Pellentesque", "mollis", "ut", "neque", "vitae", "suscipit",

            "Fusce", "at", "erat", "quis", "risus", "auctor", "posuere.", "Quisque", "eget", "fermentum", "elit,", "vel", "elementum", "metus.", "Mauris", "turpis", "metus,", "ultricies", "vel", "sapien", "non,", "sodales", "rutrum", "nibh.", "Vivamus", "id", "ante", "mauris.", "Ut", "nisi", "tortor,", "luctus", "in", "quam", "eget,", "tincidunt", "venenatis", "massa.", "Phasellus", "viverra", "mollis", "turpis", "nec", "cursus.", "Proin", "elementum", "tortor", "justo,", "vel", "accumsan", "eros", "bibendum", "at.", "Praesent", "placerat", "nisl", "mauris,", "consectetur", "interdum", "nulla", "venenatis", "vitae.", "Suspendisse", "odio", "elit,", "pellentesque", "ac", "ex", "a,", "fermentum", "iaculis", "ex.", "Pellentesque", "ac", "nunc", "mauris.", "Duis", "in", "enim", "placerat", "dui", "condimentum", "pellentesque.", "Aliquam", "feugiat,", "tortor", "nec", "interdum", "interdum,", "magna", "sapien", "tempor", "dui,", "ac", "efficitur", "justo", "est", "vitae", "lorem.", "Maecenas", "vulputate", "massa", "a", "ipsum", "luctus,", "vel", "tincidunt", "nulla", "tempor.", "Nullam", "vel", "lectus", "rhoncus,", "vehicula", "nisi", "eu,", "aliquam", "arcu.", "Proin", "in", "tempor", "dolor.", "In", "lacus", "lorem,", "dignissim", "quis", "semper", "vitae,", "rutrum", "feugiat", "sapien",

            "Mauris", "ornare", "iaculis", "sem,", "non", "consectetur", "augue", "rutrum", "convallis.", "Vivamus", "tincidunt", "ornare", "fermentum.", "Quisque", "egestas", "mauris", "vitae", "ante", "congue,", "ac", "rhoncus", "tortor", "ultricies.", "In", "augue", "nulla,", "posuere", "et", "volutpat", "quis,", "vehicula", "at", "nibh"};

    private static final String tweetAsRawJson = "{" + "\"created_at\":\"{0}\"," + "\"id\":\"{1}\"," + "\"text\":\"{2}\"," + "\"user\":{\"id\":\"{3}\"}" + "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }


    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeyWords().toArray(new String[0]);
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mocking filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {

        //submit method will run on a new thread and run the code in this thread instead of the main thread
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsJson = getFormatTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                LOG.error("Error creating twitter status!", e);
            }
        });


    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!!", e);
        }
    }

    private String getFormatTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))};

        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }

        return tweet.toString().trim();
    }
}
