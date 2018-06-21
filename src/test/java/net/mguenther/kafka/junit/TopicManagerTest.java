package net.mguenther.kafka.junit;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.TopicConfig;
import org.junit.Rule;
import org.junit.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.assertj.core.api.Assertions.assertThat;

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
}
