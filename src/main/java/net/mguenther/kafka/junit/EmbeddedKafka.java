package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;

import static org.apache.kafka.common.network.ListenerName.forSecurityProtocol;

@Slf4j
public class EmbeddedKafka implements EmbeddedLifecycle {

    private static final int UNDEFINED_BOUND_PORT = -1;

    private final int brokerId;

    private final Properties brokerConfig;

    private final Path logDirectory;

    private KafkaServer kafka;

    private int boundPort = UNDEFINED_BOUND_PORT;

    public EmbeddedKafka(final int brokerId, final EmbeddedKafkaConfig config, final String zooKeeperConnectUrl) throws IOException {
        this.brokerId = brokerId;
        this.brokerConfig = new Properties();
        this.brokerConfig.putAll(config.getBrokerProperties());
        this.brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zooKeeperConnectUrl);
        this.logDirectory = Files.createTempDirectory("kafka-junit");
        this.brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), brokerId);
        this.brokerConfig.put(KafkaConfig$.MODULE$.LogDirProp(), logDirectory.toFile().getAbsolutePath());
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

            if (boundPort != UNDEFINED_BOUND_PORT) {
                this.brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), String.valueOf(boundPort));
            }

            final KafkaConfig config = new KafkaConfig(brokerConfig, true);
            kafka = TestUtils.createServer(config, Time.SYSTEM);
            boundPort = kafka.boundPort(config.interBrokerListenerName());

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

        log.info("Removing working directory at {}. This directory contains Kafka logs for Kafka broker with ID {} as well.", logDirectory, brokerId);
        try {
            recursivelyDelete(logDirectory);
        } catch (IOException e) {
            log.warn("Unable to remove working directory at {}.", logDirectory);
        }
        log.info("The embedded Kafka broker with ID {} has been stopped.", brokerId);
    }

    private void recursivelyDelete(final Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file,
                                             @SuppressWarnings("unused") BasicFileAttributes attrs) {

                file.toFile().delete();
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                                                     @SuppressWarnings("unused") BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                dir.toFile().delete();
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void deactivate() {
        if (kafka == null) return;
        log.info("The embedded Kafka broker with ID {} is stopping.", brokerId);
        kafka.shutdown();
        kafka.awaitShutdown();
        kafka = null;
    }

    public String getBrokerList() {
        return String.format("%s:%s", kafka.config().hostName(), kafka.boundPort(forSecurityProtocol(SecurityProtocol.PLAINTEXT)));
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
