package net.mguenther.kafka.junit;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedZooKeeperConfigTest {

    @Test
    public void useDefaultsShouldUseRandomPort() {
        final EmbeddedZooKeeperConfig config = EmbeddedZooKeeperConfig.defaultZooKeeper();
        assertThat(config.getPort()).isEqualTo(EmbeddedZooKeeperConfig.USE_RANDOM_ZOOKEEPER_PORT);
    }

    @Test
    public void withPortShouldOverrideDefaultPort() {
        final EmbeddedZooKeeperConfig config = EmbeddedZooKeeperConfig
                .zooKeeper()
                .withPort(8090)
                .build();
        assertThat(config.getPort()).isEqualTo(8090);
    }
}
