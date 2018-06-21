package net.mguenther.kafka.junit;

import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class PropsTest {

    @Test
    public void buildShouldRetainPreviouslyAddedPropertiesUsingWith() {
        final Properties props = Props.create().with("test", "some-value").build();
        assertThat(props.getProperty("test")).isEqualTo("some-value");
    }

    @Test
    public void buildShouldRetainPreviouslyAddedPropertiesUsingWithAll() {
        final Properties props = Props.create().with("test", "some-value").build();
        final Properties withAllProps = Props.create().withAll(props).build();
        assertThat(withAllProps.getProperty("test")).isEqualTo("some-value");
    }
}
