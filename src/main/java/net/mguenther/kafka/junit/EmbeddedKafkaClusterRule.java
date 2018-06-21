package net.mguenther.kafka.junit;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.rules.ExternalResource;

import java.util.List;

/**
 * @deprecated since 0.2.0 - use {@link EmbeddedKafkaCluster} instead
 */
@Deprecated
@RequiredArgsConstructor
public class EmbeddedKafkaClusterRule extends ExternalResource implements RecordProducer, RecordConsumer, TopicManager {

    private final EmbeddedKafkaClusterConfig config;

    private EmbeddedKafkaCluster cluster;

    @Override
    protected void before() throws Throwable {
        cluster = new EmbeddedKafkaCluster(config);
        cluster.start();
    }

    @Override
    protected void after() {
        cluster.stop();
    }

    public static EmbeddedKafkaClusterRule provisionWith(final EmbeddedKafkaClusterConfig config) {
        return new EmbeddedKafkaClusterRule(config);
    }

    @Override
    public <V> List<V> readValues(final ReadKeyValues<String, V> readRequest) {
        return cluster.readValues(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) {
        return cluster.read(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> observe(final  ObserveKeyValues<K, V> observeRequest) throws InterruptedException {
        return cluster.observe(observeRequest);
    }

    @Override
    public <V> List<V> observeValues(final ObserveKeyValues<String, V> observeRequest) throws InterruptedException {
        return cluster.observeValues(observeRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValues<K, V> sendRequest) throws InterruptedException {
        return cluster.send(sendRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValuesTransactional<K, V> sendRequest) throws InterruptedException {
        return cluster.send(sendRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValues<V> sendRequest) throws InterruptedException {
        return cluster.send(sendRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValuesTransactional<V> sendRequest) throws InterruptedException {
        return cluster.send(sendRequest);
    }

    @Override
    public void createTopic(final TopicConfig config) {
        cluster.createTopic(config);
    }

    @Override
    public void deleteTopic(final String topic) {
        cluster.deleteTopic(topic);
    }

    @Override
    public boolean exists(final String topic) {
        return cluster.exists(topic);
    }

    public String getBrokerList() {
        return cluster.getBrokerList();
    }
}
