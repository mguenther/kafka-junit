package net.mguenther.kafka.junit.provider;

import kafka.api.LeaderAndIsr;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.TopicConfig;
import net.mguenther.kafka.junit.TopicManager;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class NoOpTopicManager implements TopicManager {

    @Override
    public void createTopic(final TopicConfig config) {
        log.warn("No ZK Connection URL has been given. Discarding this request to create a new topic with parameters {}.", config);
    }

    @Override
    public void deleteTopic(final String topic) {
        log.warn("No ZK Connection URL has been given. Discarding this request to delete topic {}.", topic);
    }

    @Override
    public boolean exists(final String topic) {
        log.warn("No ZK Connection URL has been given. Discarding this request for topic existence of topic {}.", topic);
        return false;
    }

    @Override
    public Map<Integer, LeaderAndIsr> fetchLeaderAndIsr(final String topic) {
        log.warn("No ZK Connection URL has been given. Discarding this request for broker assignments to topic-partitions for topic {}.", topic);
        return Collections.emptyMap();
    }

    @Override
    public Properties fetchTopicConfig(String topic) {
        log.warn("No ZK Connection URL has been given. Discarding this request and returning an empty instance of java.util.Properties.");
        return new Properties();
    }
}
