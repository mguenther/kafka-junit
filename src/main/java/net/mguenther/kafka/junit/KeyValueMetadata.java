package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class KeyValueMetadata {

    private final String topic;
    private final int partition;
    private final long offset;
}
