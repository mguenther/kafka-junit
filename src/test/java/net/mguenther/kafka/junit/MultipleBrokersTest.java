package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import kafka.server.KafkaConfig$;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Slf4j
public class MultipleBrokersTest {

    @Rule
    public EmbeddedKafkaCluster cluster = provisionWith(EmbeddedKafkaClusterConfig.create()
                    .provisionWith(EmbeddedKafkaConfig.create()
                            .withNumberOfBrokers(3)
                            .with(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp(), "1")
                            .with(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp(), "1")
                            .build())
                    .build());

    @Test
    public void multipleBrokersCompriseTheInSyncReplicaSetOfTopics() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .build());

        delay(5);

        final Set<Integer> leaders = leaders("test-topic");

        assertThat(leaders.size()).isEqualTo(3);
        assertThat(leaders.contains(1)).isTrue();
        assertThat(leaders.contains(2)).isTrue();
        assertThat(leaders.contains(3)).isTrue();
    }

    @Test
    public void disconnectedBrokerLeavesIsrOfTopicAndRejoinsItAfterReconnecting() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .build());

        delay(5);

        Set<Integer> leaders = leaders("test-topic");

        assertThat(leaders.contains(1)).isTrue();
        assertThat(leaders.contains(2)).isTrue();
        assertThat(leaders.contains(3)).isTrue();

        cluster.disconnect(1);

        delay(5);

        Set<Integer> leadersAfterDisconnect = leaders("test-topic");

        assertThat(leadersAfterDisconnect.contains(1)).isFalse();
        assertThat(leadersAfterDisconnect.contains(2)).isTrue();
        assertThat(leadersAfterDisconnect.contains(3)).isTrue();

        cluster.connect(1);

        delay(5);

        Set<Integer> leadersAfterReconnect = leaders("test-topic");

        assertThat(leadersAfterReconnect.contains(1)).isTrue();
        assertThat(leadersAfterReconnect.contains(2)).isTrue();
        assertThat(leadersAfterReconnect.contains(3)).isTrue();
    }

    @Test(expected = NotEnoughReplicasException.class)
    public void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingValues() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2")
                .build());

        delay(5);

        cluster.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        cluster.send(SendValues.to("test-topic", "A").useDefaults());
    }

    @Test(expected = NotEnoughReplicasException.class)
    public void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingValuesTransactionally() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2")
                .build());

        delay(5);

        cluster.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        cluster.send(SendValuesTransactional.inTransaction("test-topic", "A")
                .with(ProducerConfig.RETRIES_CONFIG, 1)
                .build());
    }

    @Test(expected = NotEnoughReplicasException.class)
    public void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingKeyValues() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2")
                .build());

        delay(5);

        cluster.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        cluster.send(SendKeyValues.to("test-topic", singletonList(new KeyValue<>("a", "A"))).useDefaults());
    }

    @Test(expected = NotEnoughReplicasException.class)
    public void disconnectUntilIsrFallsBelowMinimumSizeShouldThrowNotEnoughReplicasExceptionWhenSendingKeyValuesTransactionally() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2")
                .build());

        delay(5);

        cluster.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        cluster.send(SendKeyValuesTransactional.inTransaction("test-topic", singletonList(new KeyValue<>("a", "A")))
                .with(ProducerConfig.RETRIES_CONFIG, 1)
                .build());
    }

    @Test
    public void shouldBeAbleToWriteRecordsAfterRestoringDisconnectedIsr() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(3)
                .with("min.insync.replicas", "2")
                .build());

        delay(5);

        Set<Integer> disconnectedBrokers = cluster.disconnectUntilIsrFallsBelowMinimumSize("test-topic");

        delay(5);

        try {
            cluster.send(SendValues.to("test-topic", "A").useDefaults());
            fail("A NotEnoughReplicasException is expected, but has not been raised.");
        } catch (NotEnoughReplicasException e) {
            // ignore, this is expected
        }

        cluster.connect(disconnectedBrokers);

        delay(5);

        cluster.send(SendValues.to("test-topic", "A").useDefaults());
        cluster.observeValues(ObserveKeyValues.on("test-topic", 1).useDefaults());
    }

    private Set<Integer> leaders(final String topic) {
        return cluster.fetchLeaderAndIsr(topic)
                .values()
                .stream()
                .peek(leaderAndIsr -> log.info("Assignment: {}", leaderAndIsr.toString()))
                .map(LeaderAndIsr::leader)
                .collect(Collectors.toSet());
    }
}
