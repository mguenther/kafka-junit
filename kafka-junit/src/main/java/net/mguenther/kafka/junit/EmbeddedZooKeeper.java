package net.mguenther.kafka.junit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

@Slf4j
@RequiredArgsConstructor
public class EmbeddedZooKeeper implements EmbeddedLifecycle {

    private final EmbeddedZooKeeperConfig config;

    private TestingServer internalServer;

    @Override
    public void start() {

        if (internalServer != null) {
            log.info("The embedded ZooKeeper instance is already running.");
            return;
        }

        try {
            log.info("Embedded ZooKeeper is starting.");
            internalServer = new TestingServer(config.getPort());
            log.info("Successfully started an embedded ZooKeeper instance at {} which is assigned to the temporary directory {}.",
                    internalServer.getConnectString(),
                    internalServer.getTempDirectory());
        } catch (Exception e) {
            throw new RuntimeException("Unable to start an embedded ZooKeeper instance.", e);
        }
    }

    @Override
    public void stop() {

        if (internalServer == null) {
            log.info("The embedded ZooKeeper is not running or was already shut down.");
            return;
        }

        try {
            log.info("The embedded ZooKeeper instance at {} is stopping.", internalServer.getConnectString());
            internalServer.close();
            log.info("The embedded ZooKeeper instance at {} has been shut down.", internalServer.getConnectString());
        } catch (Exception e) {
            throw new RuntimeException("Unable to stop the embedded ZooKeeper instance.", e);
        }
    }

    public String getConnectString() {
        return internalServer.getConnectString();
    }
}
