package net.mguenther.kafka.junit;

import java.util.Properties;

/**
 * Provides a fluent interface for constructing {@link java.util.Properties}. Use this for example
 * with {@code EmbeddedConnectConfig#deployConnector} to retain the fluent interface when provisioning
 * your embedded Kafka cluster.
 */
public class Props {

    private final Properties properties = new Properties();

    public <T> Props with(final String propertyName, final T value) {
        properties.put(propertyName, value);
        return this;
    }

    public Props withAll(final Properties overrides) {
        properties.putAll(overrides);
        return this;
    }

    public Properties build() {
        final Properties copyOfProps = new Properties();
        copyOfProps.putAll(properties);
        return copyOfProps;
    }

    public static Props create() {
        return new Props();
    }
}
