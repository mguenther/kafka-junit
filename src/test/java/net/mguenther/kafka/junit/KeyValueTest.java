package net.mguenther.kafka.junit;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyValueTest {

    @Test
    @DisplayName("should preserve added headers")
    public void shouldPreserveAddedHeaders() {

        final KeyValue<String, String> keyValue = new KeyValue<>("k", "v");
        keyValue.addHeader("headerName", "headerValue", StandardCharsets.UTF_8);

        assertThat(keyValue.getHeaders().lastHeader("headerName").value()).isEqualTo("headerValue".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("should preserve headers given on construction")
    public void shouldPreserveHeadersGivenOnConstruction() {

        final Headers headers = new RecordHeaders();
        headers.add("headerName", "headerValue".getBytes(StandardCharsets.UTF_8));
        final KeyValue<String, String> keyValue = new KeyValue<>("k", "v", headers);

        assertThat(keyValue.getHeaders().lastHeader("headerName").value()).isEqualTo("headerValue".getBytes(StandardCharsets.UTF_8));
    }
}
