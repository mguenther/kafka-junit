package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TopicManagerTest {

    @Rule
    public EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

    @Test
    public void manageTopics() {

        cluster.createTopic(TopicConfig.forTopic("test-topic").useDefaults());

        assertThat(cluster.exists("test-topic")).isTrue();

        // the topic will not be deleted immediately, but "marked for deletion"
        // hence a check on exists would return "false" directly after deleting
        // the topic
        cluster.deleteTopic("test-topic");
    }

    @Test
    public void getLeaderAndIsrShouldRetrieveTheIsr() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(1)
                .build());

        // it takes a couple of seconds until topic-partition assignments are there
        delay(5);

        Map<Integer, LeaderAndIsr> isr = cluster.getLeaderAndIsr("test-topic");

        assertThat(isr.size()).isEqualTo(5);
        assertThat(isr.values().stream().allMatch(lai -> lai.leader() == 1001)).isTrue();
    }
}
