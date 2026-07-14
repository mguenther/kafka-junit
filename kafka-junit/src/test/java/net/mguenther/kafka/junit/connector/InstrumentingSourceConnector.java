package net.mguenther.kafka.junit.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class InstrumentingSourceConnector extends SourceConnector {

    private InstrumentingConfig config;

    @Override
    public String version() {
        return StringUtils.EMPTY;
    }

    @Override
    public void start(final Map<String, String> props) {
        config = new InstrumentingConfig(config(), props);
        log.info("Starting InstrumentingSourceConnector using configuration '{}'.", config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return InstrumentingSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int i) {
        return Collections.singletonList(config.originalsStrings());
    }

    @Override
    public void stop() {
        log.info("Stopping InstrumentingSourceConnector.");
    }

    @Override
    public ConfigDef config() {
        return InstrumentingConfig.config();
    }
}
