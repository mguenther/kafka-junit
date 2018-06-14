package net.mguenther.kafka.junit;

import org.junit.Rule;
import org.junit.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterRule.provisionWith;
import static org.assertj.core.api.Assertions.assertThat;

public class TopicManagerTest {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

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
