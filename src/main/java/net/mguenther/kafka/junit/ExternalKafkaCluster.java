package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import net.mguenther.kafka.junit.provider.DefaultRecordProducer;
import net.mguenther.kafka.junit.provider.DefaultTopicManager;
import net.mguenther.kafka.junit.provider.NoOpTopicManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class ExternalKafkaCluster implements RecordProducer, RecordConsumer, TopicManager {

    private final RecordProducer producerDelegate;

    private final RecordConsumer consumerDelegate;

    private final TopicManager topicManagerDelegate;

    private ExternalKafkaCluster(final String bootstrapServers) {
        this(bootstrapServers, StringUtils.EMPTY);
    }

    private ExternalKafkaCluster(final String bootstrapServers, final String zkConnectString) {
        producerDelegate = new DefaultRecordProducer(bootstrapServers);
        consumerDelegate = new DefaultRecordConsumer(bootstrapServers);
        topicManagerDelegate = StringUtils.isEmpty(zkConnectString) ?
                new NoOpTopicManager() : new DefaultTopicManager(zkConnectString);
    }

    @Override
    public <V> List<V> readValues(final ReadKeyValues<String, V> readRequest) throws InterruptedException {
        return consumerDelegate.readValues(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) throws InterruptedException {
        return consumerDelegate.read(readRequest);
    }

    @Override
    public <V> List<V> observeValues(final ObserveKeyValues<String, V> observeRequest) throws InterruptedException {
        return consumerDelegate.observeValues(observeRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> observe(final ObserveKeyValues<K, V> observeRequest) throws InterruptedException {
        return consumerDelegate.observe(observeRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValues<V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValuesTransactional<V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValues<K, V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValuesTransactional<K, V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public void createTopic(final TopicConfig config) {
        topicManagerDelegate.createTopic(config);
    }

    @Override
    public void deleteTopic(final String topic) {
        topicManagerDelegate.deleteTopic(topic);
    }

    @Override
    public boolean exists(final String topic) {
        return topicManagerDelegate.exists(topic);
    }

    @Override
    public Map<Integer, LeaderAndIsr> fetchLeaderAndIsr(final String topic) {
        return topicManagerDelegate.fetchLeaderAndIsr(topic);
    }

    @Override
    public Properties fetchTopicConfig(final String topic) {
        return topicManagerDelegate.fetchTopicConfig(topic);
    }

    public static ExternalKafkaCluster at(final String bootstrapServers) {
        return new ExternalKafkaCluster(bootstrapServers);
    }

    public static ExternalKafkaCluster at(final String bootstrapServers, final String zkConnectString) {
        return new ExternalKafkaCluster(bootstrapServers, zkConnectString);
    }
}
