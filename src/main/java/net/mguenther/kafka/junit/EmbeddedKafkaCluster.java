package net.mguenther.kafka.junit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import net.mguenther.kafka.junit.provider.DefaultRecordProducer;
import net.mguenther.kafka.junit.provider.DefaultTopicManager;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.rules.ExternalResource;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class EmbeddedKafkaCluster extends ExternalResource implements EmbeddedLifecycle, RecordProducer, RecordConsumer, TopicManager, AutoCloseable {

    private final EmbeddedKafkaClusterConfig config;

    private EmbeddedZooKeeper zooKeeper;

    private EmbeddedKafka broker;

    private EmbeddedConnect connect;

    private RecordProducer producerDelegate;

    private RecordConsumer consumerDelegate;

    private TopicManager topicManagerDelegate;

    @Override
    protected void before() throws Throwable {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    @Override
    public void start() {

        try {

            zooKeeper = new EmbeddedZooKeeper(config.getZooKeeperConfig());
            zooKeeper.start();

            broker = new EmbeddedKafka(config.getKafkaConfig(), zooKeeper.getConnectString());
            broker.start();

            if (config.usesConnect()) {
                connect = new EmbeddedConnect(config.getConnectConfig(), getBrokerList());
                connect.start();
            }

            producerDelegate = new DefaultRecordProducer(getBrokerList());
            consumerDelegate = new DefaultRecordConsumer(getBrokerList());
            topicManagerDelegate = new DefaultTopicManager(zooKeeper.getConnectString());

        } catch (Exception e) {
            throw new RuntimeException("Unable to start the embedded Kafka cluster.", e);
        }
    }

    @Override
    public void stop() {

        if (connect != null) {
            connect.stop();
        }

        broker.stop();
        zooKeeper.stop();
    }

    public String getBrokerList() {
        return broker.getBrokerList();
    }

    public static EmbeddedKafkaCluster provisionWith(final EmbeddedKafkaClusterConfig config) {
        return new EmbeddedKafkaCluster(config);
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
    public <V> List<RecordMetadata> send(final SendValues<V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValuesTransactional<V> sendRequest) throws InterruptedException {
        return producerDelegate.send(sendRequest);
    }

    @Override
    public <V> List<V> readValues(final ReadKeyValues<String, V> readRequest) {
        return consumerDelegate.readValues(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) {
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
    public void createTopic(final TopicConfig topicConfig) {
        topicManagerDelegate.createTopic(topicConfig);
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
    public void close() throws Exception {
        stop();
    }
}
