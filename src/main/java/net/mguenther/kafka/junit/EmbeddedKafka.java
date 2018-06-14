package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.common.network.ListenerName.forSecurityProtocol;

@Slf4j
public class EmbeddedKafka implements EmbeddedLifecycle {

    private final Properties brokerConfig;

    private final TemporaryFolder workingDirectory;

    private final File logDirectory;

    private KafkaServer kafka;

    public EmbeddedKafka(final EmbeddedKafkaConfig config, final String zooKeeperConnectUrl) throws IOException {
        this.brokerConfig = new Properties();
        this.brokerConfig.putAll(config.getBrokerProperties());
        this.brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zooKeeperConnectUrl);
        this.workingDirectory = new TemporaryFolder();
        this.workingDirectory.create();
        this.logDirectory = this.workingDirectory.newFolder();
        this.brokerConfig.put(KafkaConfig$.MODULE$.LogDirProp(), logDirectory.getAbsolutePath());
    }

    @Override
    public void start() {

        if (kafka != null) {
            log.info("The embedded Kafka broker is already running.");
            return;
        }

        try {
            log.info("Embedded Kafka broker is starting.");

            final KafkaConfig config = new KafkaConfig(brokerConfig, true);
            kafka = TestUtils.createServer(config, Time.SYSTEM);

            log.info("The embedded Kafka broker has been started. Its logs can be found at {}.", logDirectory);
        } catch (Exception e) {
            throw new RuntimeException("Unable to start the embedded Kafka broker.", e);
        }
    }

    @Override
    public void stop() {

        if (kafka == null) {
            log.info("The embedded Kafka broker is not running or was already shut down.");
            return;
        }

        log.info("The embedded Kafka broker is stopping.");
        kafka.shutdown();
        kafka.awaitShutdown();
        log.info("Removing working directory at {}. This directory contains Kafka logs as well.", workingDirectory);
        workingDirectory.delete();
        log.info("The embedded Kafka broker has been stopped.");
    }

    public String getBrokerList() {
        return String.format("%s:%s", kafka.config().hostName(), Integer.toString(kafka.boundPort(forSecurityProtocol(SecurityProtocol.PLAINTEXT))));
    }
}
