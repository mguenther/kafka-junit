package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.ToString;

@ToString
public class EmbeddedZooKeeperConfig {

    public static final int USE_RANDOM_ZOOKEEPER_PORT = -1;

    public static class EmbeddedZooKeeperConfigBuilder {

        private int port = USE_RANDOM_ZOOKEEPER_PORT;

        EmbeddedZooKeeperConfigBuilder withPort(final int port) {
            this.port = port;
            return this;
        }

        public EmbeddedZooKeeperConfig build() {
            return new EmbeddedZooKeeperConfig(this);
        }
    }

    @Getter
    private final Integer port;

    private EmbeddedZooKeeperConfig(final EmbeddedZooKeeperConfigBuilder builder) {
        this.port = builder.port;
    }

    public static EmbeddedZooKeeperConfigBuilder create() {
        return new EmbeddedZooKeeperConfigBuilder();
    }

    public static EmbeddedZooKeeperConfig useDefaults() {
        return create().build();
    }
}
