package net.mguenther.kafka.junit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.Charset;
import java.util.Optional;

@ToString
@EqualsAndHashCode(of = { "key", "value" })
@RequiredArgsConstructor
public class KeyValue<K, V> {

    @Getter
    private final K key;

    @Getter
    private final V value;

    @Getter
    private final Headers headers;

    private final KeyValueMetadata metadata;

    public KeyValue(final K key, final V value) {
        this(key, value, new RecordHeaders(), null);
    }

    public KeyValue(final K key, final V value, final Headers headers) {
        this(key, value, headers, null);
    }

    public void addHeader(final String headerName, final String headerValue, final Charset charset) {
        addHeader(headerName, headerValue.getBytes(charset));
    }

    public void addHeader(final String headerName, final byte[] headerValue) {
        final Header header = new RecordHeader(headerName, headerValue);
        headers.add(header);
    }

    public Optional<KeyValueMetadata> getMetadata() {
        return Optional.ofNullable(metadata);
    }
}
