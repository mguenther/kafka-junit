package net.mguenther.kafka.junit.connector;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static java.util.Collections.emptyList;

@ToString
public class InstrumentingConfig extends AbstractConfig {

    @Getter
    private final String topic;

    @Getter
    private final String key;

    public InstrumentingConfig(final ConfigDef configDef, final Map<String, ?> originals) {
        super(configDef, originals, true);

        topic = getString("topic");
        key = getString("key");
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(toKey("topic", ConfigDef.Type.STRING))
                .define(toKey("key", ConfigDef.Type.STRING));
    }

    private static ConfigDef.ConfigKey toKey(final String name, final ConfigDef.Type type) {
        return new ConfigDef.ConfigKey(name, type, null, null, ConfigDef.Importance.HIGH, StringUtils.EMPTY, null, -1, ConfigDef.Width.NONE, null, emptyList(), null, false);
    }
}