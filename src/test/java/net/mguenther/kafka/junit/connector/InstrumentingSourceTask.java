package net.mguenther.kafka.junit.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static net.mguenther.kafka.junit.connector.InstrumentingConfig.config;

@Slf4j
public class InstrumentingSourceTask extends SourceTask {

    private final AtomicInteger currentSourceOffset = new AtomicInteger(0);

    private final String key = UUID.randomUUID().toString();

    private InstrumentingConfig config;

    @Override
    public void start(final Map<String, String> props) {
        config = new InstrumentingConfig(config(), props);
        log.info("Starting task InstrumentingSourceTask using configuration '{}'.", config);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        return Collections.singletonList(new SourceRecord(
                sourcePartition(),
                sourceOffset(),
                config.getTopic(),
                0,
                null,
                config.getKey(),
                null,
                UUID.randomUUID().toString(),
                System.currentTimeMillis()));
    }

    private Map<String, ?> sourcePartition() {
        return Collections.singletonMap("source", "instrumenting-source-record-partition");
    }

    private Map<String, ?> sourceOffset() {
        final Integer sourceOffset =  currentSourceOffset.getAndIncrement();
        return Collections.singletonMap("offset", String.valueOf(sourceOffset));
    }

    @Override
    public void stop() {
        log.info("Stopping task InstrumentingSourceTask.");
    }

    @Override
    public String version() {
        return StringUtils.EMPTY;
    }
}
