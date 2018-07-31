package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import kafka.server.KafkaConfig$;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import net.mguenther.kafka.junit.provider.DefaultRecordProducer;
import net.mguenther.kafka.junit.provider.DefaultTopicManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.rules.ExternalResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

    /**
     * @return
     *      Collects the addresses of all active brokers in the cluster and joins them together
     *      by using ',' as a delimiter. The resulting {@code String} can be used to configure
     *      the bootstrap servers parameter of Kafka producers and consumers.
     */
    public String getBrokerList() {
        final List<String> brokerAddresses = brokers.values().stream()
                .filter(EmbeddedKafka::isActive)
                .map(EmbeddedKafka::getBrokerList)
                .collect(Collectors.toList());
        return StringUtils.join(brokerAddresses, ",");
    }

    /**
     * @return
     *      the ID of the embedded cluster
     */
    public String getClusterId() {
        return brokers.values().stream()
                .map(EmbeddedKafka::getClusterId)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    /**
     * Disconnects the broker identified by the given broker ID by deactivating it. Logs
     * etc. will be kept so that the disconnected broker can be reactivated (cf. {@code connect} method).
     * Returns immediately without changing cluster membership if there is no broker with that ID.
     *
     * @param brokerId
     *      identifies the embedded Kafka broker that ought to be disconnected
     */
    public void disconnect(final Integer brokerId) {

        if (!brokers.containsKey(brokerId)) {
            log.info("There is no broker with ID {}. Omitting the disconnect request.", brokerId);
            return;
        }

        final EmbeddedKafka broker = brokers.get(brokerId);
        broker.deactivate();
    }

    /**
     * (Re-)Connects the broker identified by the given broker ID by activating it again. Returns
     * immediately without changing cluster membership if there is no broker with that ID.
     *
     * @param brokerId
     *      identifies the embedded Kafka broker that ought to be connected
     */
    public void connect(final Integer brokerId) {

        if (!brokers.containsKey(brokerId)) {
            log.info("There is no broker with ID {}. Omitting the connection request.", brokerId);
            return;
        }

        final EmbeddedKafka broker = brokers.get(brokerId);
        broker.activate();
    }

    /**
     * (Re-)Connects all embedded Kafka brokers for the given broker IDs.
     *
     * @param brokerIds
     *      {@link java.util.Set} of broker IDs
     */
    public void connect(final Set<Integer> brokerIds) {
        brokerIds.forEach(this::connect);
    }

    /**
     * Shrinks the In-Sync-Replica Set (ISR) for the given topic by disconnecting leaders until the size
     * of the ISR falls below the minimum ISR size for that topic. Fetches the minimum ISR size via the
     * topic configuration of the given topic obtained from the cluster. If the topic configuration does
     * not provide an overridden value of {@code min.insync.replicas}, then a default value of 1 is assumed.
     * This is fine, since in this case all leaders will be disconnected, blocking all reads and writes
     * to the topic in any case.
     *
     * @param topic
     *      the name of the topic for which the ISR should fall below its minimum size
     * @throws RuntimeException
     *      in case fetching the topic configuration fails
     * @return
     *      unmodifiable {@link java.util.Set} of broker IDs that have been disconnected during the operation,
     *      so that they can be re-connected afterwards to restore the ISR
     */
    public Set<Integer> disconnectUntilIsrFallsBelowMinimumSize(final String topic) {
        final Properties topicConfig = topicManagerDelegate.fetchTopicConfig(topic);
        final int minimumIsrSize = Integer.valueOf(topicConfig.getProperty(KafkaConfig$.MODULE$.MinInSyncReplicasProp(), "1"));
        log.info("Attempting to drop the number of brokers in the ISR for topic {} below {}.", topic, minimumIsrSize);
        final Set<Integer> disconnectedBrokers = new HashSet<>();
        final Set<Integer> leaders = topicManagerDelegate.fetchLeaderAndIsr(topic)
                .values()
                .stream()
                .map(LeaderAndIsr::leader)
                .collect(Collectors.toSet());
        log.info("Active brokers ({}) in the ISR for topic {} are: {}", leaders.size(), topic, StringUtils.join(leaders, ", "));
        int currentSizeOfIsr = leaders.size();
        while (currentSizeOfIsr >= minimumIsrSize) {
            final Integer brokerId = leaders.stream().limit(1).findFirst().get(); // safe get, otherwise loop condition would fail
            disconnect(brokerId);
            disconnectedBrokers.add(brokerId);
            leaders.remove(brokerId);
            currentSizeOfIsr -= 1;
            log.info("Disconnected broker with ID {}. The current size of the ISR for topic {} is {}.", brokerId, topic, currentSizeOfIsr);
        }
        return Collections.unmodifiableSet(disconnectedBrokers);
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
    public Map<Integer, LeaderAndIsr> fetchLeaderAndIsr(final String topic) {
        return topicManagerDelegate.fetchLeaderAndIsr(topic);
    }

    @Override
    public Properties fetchTopicConfig(final String topic) {
        return topicManagerDelegate.fetchTopicConfig(topic);
    }

    @Override
    public void close() {
        stop();
    }
}
