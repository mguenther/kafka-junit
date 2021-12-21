package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.EmbeddedKafkaConfig.brokers;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class SingleBrokerTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareEnvironment() {
        kafka = provisionWith(newClusterConfig()
                .configure(brokers()
                        .withNumberOfBrokers(1)
                        .with(KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://localhost:9093")));
        kafka.start();
    }

    @AfterEach
    void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("should be able to override listener and switch to another local port")
    void shouldBeAbleToOverrideListenerAndSwitchToAnotherLocalPort() {
        assertThat(kafka.getBrokerList()).contains("localhost:9093");
    }
}
