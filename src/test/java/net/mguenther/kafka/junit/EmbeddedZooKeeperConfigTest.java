package net.mguenther.kafka.junit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedZooKeeperConfigTest {

    @Test
    @DisplayName("should use a randomly chosen port per default")
    public void useDefaultsShouldUseRandomPort() {
        final EmbeddedZooKeeperConfig config = EmbeddedZooKeeperConfig.defaultZooKeeper();
        assertThat(config.getPort()).isEqualTo(EmbeddedZooKeeperConfig.USE_RANDOM_ZOOKEEPER_PORT);
    }

    @Test
    @DisplayName("withPort should override the default port")
    public void withPortShouldOverrideDefaultPort() {
        final EmbeddedZooKeeperConfig config = EmbeddedZooKeeperConfig
                .zooKeeper()
                .withPort(8090)
                .build();
        assertThat(config.getPort()).isEqualTo(8090);
    }
}
