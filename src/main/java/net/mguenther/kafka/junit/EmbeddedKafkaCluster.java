package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import net.mguenther.kafka.junit.provider.DefaultRecordProducer;
import net.mguenther.kafka.junit.provider.DefaultTopicManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.rules.ExternalResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class EmbeddedKafkaCluster extends ExternalResource implements EmbeddedLifecycle, RecordProducer, RecordConsumer, TopicManager, AutoCloseable {

    private final EmbeddedKafkaClusterConfig config;

    private EmbeddedZooKeeper zooKeeper;

    private Map<Integer, EmbeddedKafka> brokers;

    private EmbeddedConnect connect;

    private RecordProducer producerDelegate;

    private RecordConsumer consumerDelegate;

    private TopicManager topicManagerDelegate;

    @Override
    protected void before() {
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

            brokers = new HashMap<>();

            for (int i = 0; i < config.getKafkaConfig().getNumberOfBrokers(); i++) {
                final int brokerId = i + 1;
                final EmbeddedKafka broker = new EmbeddedKafka(brokerId, config.getKafkaConfig(), zooKeeper.getConnectString());
                broker.start();
                brokers.put(broker.getBrokerId(), broker);
            }

            if (config.usesConnect()) {
                connect = new EmbeddedConnect(config.getConnectConfig(), getBrokerList(), getClusterId());
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

        brokers.values().forEach(EmbeddedKafka::stop);
        zooKeeper.stop();
    }

    public String getBrokerList() {
        final List<String> brokerAddresses = brokers.values().stream()
                .filter(EmbeddedKafka::isActive)
                .map(EmbeddedKafka::getBrokerList)
                .collect(Collectors.toList());
        return StringUtils.join(brokerAddresses, ",");
    }

    public String getClusterId() {
        return brokers.values().stream()
                .map(EmbeddedKafka::getClusterId)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    public void disconnect(final Integer brokerId) {

        if (!brokers.containsKey(brokerId)) {
            log.info("There is no broker with ID {}. Omitting the disconnect request.", brokerId);
            return;
        }

        final EmbeddedKafka broker = brokers.get(brokerId);
        broker.deactivate();
    }

    public void connect(final Integer brokerId) {

        if (!brokers.containsKey(brokerId)) {
            log.info("There is no broker with ID {}. Omitting the connection request.", brokerId);
            return;
        }

        final EmbeddedKafka broker = brokers.get(brokerId);
        broker.activate();
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
    public Map<Integer, LeaderAndIsr> getLeaderAndIsr(final String topic) {
        return topicManagerDelegate.getLeaderAndIsr(topic);
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
