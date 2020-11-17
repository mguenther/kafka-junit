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

    public static EmbeddedZooKeeperConfigBuilder zooKeeper() {
        return new EmbeddedZooKeeperConfigBuilder();
    }

    /**
     * @return
     *      instance of {@link EmbeddedZooKeeperConfigBuilder}
     * @deprecated
     *      This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     *      Use {@link #zooKeeper()} instead.
     */
    @Deprecated
    public static EmbeddedZooKeeperConfigBuilder create() {
        return zooKeeper();
    }

    public static EmbeddedZooKeeperConfig defaultZooKeeper() {
        return zooKeeper().build();
    }

    /**
     * @return
     *      instance of {@link EmbeddedZooKeeperConfig} that contains the default configuration
     *      for the embedded ZooKeeper instance
     * @deprecated
     *      This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     *      Use {@link #defaultZooKeeper()} instead.
     */
    @Deprecated
    public static EmbeddedZooKeeperConfig useDefaults() {
        return defaultZooKeeper();
    }
}
