package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.EmbeddedKafkaConfig.brokers;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValuesTransactional.inTransaction;
import static net.mguenther.kafka.junit.SendValues.to;
import static net.mguenther.kafka.junit.SendValuesTransactional.inTransaction;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@Slf4j
class MultipleBrokersTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareEnvironment() {
        kafka = provisionWith(newClusterConfig()
                .configure(brokers()
                        .withNumberOfBrokers(3)
                        .with(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp(), "1")
                        .with(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp(), "1")));
        kafka.start();
    }

    @AfterEach
    void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("multiple brokers should comprise the in-sync replica set of topics")
    void multipleBrokersCompriseTheInSyncReplicaSetOfTopics() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3));

        delay(5);

        final Set<Integer> leaders = leaders("test-topic");

        assertThat(leaders.size()).isEqualTo(3);
        assertThat(leaders.contains(1)).isTrue();
        assertThat(leaders.contains(2)).isTrue();
        assertThat(leaders.contains(3)).isTrue();
    }

    @Test
    @DisplayName("disconnected broker leaves in-sync replica set of topic and rejoins it after re-joining the cluster")
    void disconnectedBrokerLeavesIsrOfTopicAndRejoinsItAfterReconnecting() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3));

        delay(5);

        Set<Integer> leaders = leaders("test-topic");

        assertThat(leaders.contains(1)).isTrue();
        assertThat(leaders.contains(2)).isTrue();
        assertThat(leaders.contains(3)).isTrue();

        kafka.disconnect(1);

        delay(5);

        Set<Integer> leadersAfterDisconnect = leaders("test-topic");

        assertThat(leadersAfterDisconnect.contains(1)).isFalse();
        assertThat(leadersAfterDisconnect.contains(2)).isTrue();
        assertThat(leadersAfterDisconnect.contains(3)).isTrue();

        kafka.connect(1);

        delay(10);

        Set<Integer> leadersAfterReconnect = leaders("test-topic");

        assertThat(leadersAfterReconnect.contains(1)).isTrue();
        assertThat(leadersAfterReconnect.contains(2)).isTrue();
        assertThat(leadersAfterReconnect.contains(3)).isTrue();
    }

    @Test
    @DisplayName("should throw NotEnoughReplicasException when trying to send non-keyed records and ISR has fallen below its minimum size")
    void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingValues() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(5);

        kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        Assertions.assertThrows(NotEnoughReplicasException.class, () -> {
            kafka.send(to("test-topic", "A"));
        });
    }

    @Test
    @DisplayName("should throw NotEnoughReplicasException when trying to send non-keyed records within a transaction and ISR has fallen below its minimum size")
    void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingValuesTransactionally() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(5);

        kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        Assertions.assertThrows(NotEnoughReplicasException.class, () -> {
            kafka.send(inTransaction("test-topic", "A")
                    .with(ProducerConfig.RETRIES_CONFIG, 1));
        });
    }

    @Test
    @DisplayName("should throw NotEnoughReplicasException when trying to send keyed records and ISR has fallen below its minimum size")
    void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingKeyValues() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(5);

        kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        Assertions.assertThrows(NotEnoughReplicasException.class, () -> {
            kafka.send(SendKeyValues.to("test-topic", singletonList(new KeyValue<>("a", "A"))));
        });
    }

    @Test
    @DisplayName("should throw NotEnoughReplicasException when trying to send keyed records within a transaction and ISR has fallen below its minimum size")
    void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingKeyValuesTransactionally() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(5);

        kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        Assertions.assertThrows(NotEnoughReplicasException.class, () -> {
            kafka.send(inTransaction("test-topic", singletonList(new KeyValue<>("a", "A")))
                    .with(ProducerConfig.RETRIES_CONFIG, 1));
        });
    }

    @Test
    @DisplayName("should be able to submit reords after restoring previously disconnected in-sync replica set")
    void shouldBeAbleToWriteRecordsAfterRestoringDisconnectedIsr() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        delay(5);

        final Set<Integer> disconnectedBrokers = kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        try {
            kafka.send(to("test-topic", "A"));
            fail("A NotEnoughReplicasException is expected, but has not been raised.");
        } catch (NotEnoughReplicasException e) {
            // ignore, this is expected
        }

        kafka.connect(disconnectedBrokers);

        delay(5);

        kafka.send(to("test-topic", "A"));
        kafka.observeValues(on("test-topic", 1));
    }

    @Test
    @DisplayName("a re-enabled broker should bind to the same port as it was bound before")
    void reActivatedBrokersShouldBindToTheSamePortAsTheyWereBoundToBefore() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2"));

        final List<String> brokersBeforeDisconnect = Arrays.asList(kafka.getBrokerList().split(","));

        final Set<Integer> disconnectedBrokers = kafka.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        assertThat(disconnectedBrokers.size()).isEqualTo(2);

        delay(5);

        kafka.connect(disconnectedBrokers);

        delay(5);

        final List<String> brokersAfterReconnect = Arrays.asList(kafka.getBrokerList().split(","));

        assertThat(brokersAfterReconnect).containsAll(brokersBeforeDisconnect);
        assertThat(brokersBeforeDisconnect).containsAll(brokersAfterReconnect);
    }

    private Set<Integer> leaders(final String topic) {
        return kafka.fetchLeaderAndIsr(topic)
                .values()
                .stream()
                .peek(leaderAndIsr -> log.info("Assignment: {}", leaderAndIsr.toString()))
                .map(LeaderAndIsr::getLeader)
                .collect(Collectors.toSet());
    }
}
