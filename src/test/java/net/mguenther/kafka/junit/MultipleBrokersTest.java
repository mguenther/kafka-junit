package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import kafka.server.KafkaConfig$;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

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

        final List<Integer> leaders = cluster.getLeaderAndIsr("test-topic")
                .values()
                .stream()
                .map(LeaderAndIsr::leader)
                .collect(Collectors.toList());

        assertThat(leaders.contains(1)).isTrue();
        assertThat(leaders.contains(2)).isTrue();
        assertThat(leaders.contains(3)).isTrue();
    }
}
