package net.mguenther.kafka.junit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PropsTest {

    @Test
    @DisplayName("build should retain properties that were added using a call to with")
    void buildShouldRetainPreviouslyAddedPropertiesUsingWith() {
        final Properties props = Props.create().with("test", "some-value").build();
        assertThat(props.getProperty("test")).isEqualTo("some-value");
    }

    @Test
    @DisplayName("build should retain properties that were added using a call to withAll")
    void buildShouldRetainPreviouslyAddedPropertiesUsingWithAll() {
        final Properties props = Props.create().with("test", "some-value").build();
        final Properties withAllProps = Props.create().withAll(props).build();
        assertThat(withAllProps.getProperty("test")).isEqualTo("some-value");
    }
}
