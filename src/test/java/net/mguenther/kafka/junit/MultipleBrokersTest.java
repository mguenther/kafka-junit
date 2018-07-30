package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import kafka.server.KafkaConfig$;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MultipleBrokersTest {

    @Rule
    public EmbeddedKafkaCluster cluster = provisionWith(EmbeddedKafkaClusterConfig.create()
                    .provisionWith(EmbeddedKafkaConfig.create()
                            .withNumberOfBrokers(3)
                            .with(KafkaConfig$.MODULE$.NumPartitionsProp(), "5")
                            .with(KafkaConfig$.MODULE$.DefaultReplicationFactorProp(), "3")
                            .with(KafkaConfig$.MODULE$.MinInSyncReplicasProp(), "2")
                            .with(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), "3")
                            .with(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp(), "3")
                            .with(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp(), "2")
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

    private Set<Integer> leaders(final String topic) {
        return cluster.getLeaderAndIsr(topic)
                .values()
                .stream()
                .map(LeaderAndIsr::leader)
                .collect(Collectors.toSet());
    }
}
