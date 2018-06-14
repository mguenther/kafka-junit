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

@Getter
@ToString
@EqualsAndHashCode(of = { "key", "value" })
@RequiredArgsConstructor
public class KeyValue<K, V> {

    private final K key;
    private final V value;
    private final Headers headers;

    public KeyValue(final K key, final V value) {
        this(key, value, new RecordHeaders());
    }

    public void addHeader(final String headerName, final String headerValue, final Charset charset) {
        addHeader(headerName, headerValue.getBytes(charset));
    }

    public void addHeader(final String headerName, final byte[] headerValue) {
        final Header header = new RecordHeader(headerName, headerValue);
        headers.add(header);
    }
}
