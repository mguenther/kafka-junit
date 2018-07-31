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

    private final int brokerId;

    private final Properties brokerConfig;

    private final TemporaryFolder workingDirectory;

    private final File logDirectory;

    private KafkaServer kafka;

    public EmbeddedKafka(final int brokerId, final EmbeddedKafkaConfig config, final String zooKeeperConnectUrl) throws IOException {
        this.brokerId = brokerId;
        this.brokerConfig = new Properties();
        this.brokerConfig.putAll(config.getBrokerProperties());
        this.brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zooKeeperConnectUrl);
        this.workingDirectory = new TemporaryFolder();
        this.workingDirectory.create();
        this.logDirectory = this.workingDirectory.newFolder();
        this.brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), brokerId);
        this.brokerConfig.put(KafkaConfig$.MODULE$.LogDirProp(), logDirectory.getAbsolutePath());
    }

    @Override
    public void start() {
        activate();
    }

    public void activate() {

        if (kafka != null) {
            log.info("The embedded Kafka broker with ID {} is already running.", brokerId);
            return;
        }

        try {
            log.info("Embedded Kafka broker with ID {} is starting.", brokerId);

            final KafkaConfig config = new KafkaConfig(brokerConfig, true);
            kafka = TestUtils.createServer(config, Time.SYSTEM);

            log.info("The embedded Kafka broker with ID {} has been started. Its logs can be found at {}.", brokerId, logDirectory);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to start the embedded Kafka broker with ID %s.", brokerId), e);
        }
    }

    @Override
    public void stop() {

        if (kafka == null) {
            log.info("The embedded Kafka broker with ID {} is not running or was already shut down.", brokerId);
            return;
        }

        deactivate();

        log.info("Removing working directory at {}. This directory contains Kafka logs for Kafka broker with ID {} as well.", workingDirectory, brokerId);
        workingDirectory.delete();
        log.info("The embedded Kafka broker with ID {} has been stopped.", brokerId);
    }

    public void deactivate() {
        if (kafka == null) return;
        log.info("The embedded Kafka broker with ID {} is stopping.", brokerId);
        kafka.shutdown();
        kafka.awaitShutdown();
        kafka = null;
    }

    public String getBrokerList() {
        return String.format("%s:%s", kafka.config().hostName(), Integer.toString(kafka.boundPort(forSecurityProtocol(SecurityProtocol.PLAINTEXT))));
    }

    public String getClusterId() {
        return kafka.clusterId();
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public boolean isActive() {
        return kafka != null;
    }
}
