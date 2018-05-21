package net.mguenther.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KeyValue;
import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
    public <V> List<V> readValues(ReadKeyValues<Object, V> readRequest) {
        return cluster.readValues(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) {
        return cluster.read(readRequest);
    }

    @Override
    public <K, V> List<KeyValue<K, V>> observe(ObserveKeyValues<K, V> observeRequest) throws InterruptedException {
        return cluster.observe(observeRequest);
    }

    @Override
    public <V> List<V> observeValues(ObserveKeyValues<Object, V> observeRequest) throws InterruptedException {
        return cluster.observeValues(observeRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValues<K, V> sendRequest) throws ExecutionException, InterruptedException {
        return cluster.send(sendRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValues<V> sendRequest) throws ExecutionException, InterruptedException {
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
