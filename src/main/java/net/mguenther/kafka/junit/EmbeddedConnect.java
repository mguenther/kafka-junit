package net.mguenther.kafka.junit;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.FutureCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EmbeddedConnect implements EmbeddedLifecycle {

    private static final int REQUEST_TIMEOUT_MS = 120_000;

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final List<Properties> connectorConfigs;

    private final DistributedConfig config;

    private final KafkaOffsetBackingStore offsetBackingStore;

    private final Worker worker;

    private final StatusBackingStore statusBackingStore;

    private final ConfigBackingStore configBackingStore;

    private final DistributedHerder herder;

    public EmbeddedConnect(final EmbeddedConnectConfig connectConfig, final String brokerList, final String clusterId) {
        final Properties effectiveWorkerConfig = connectConfig.getConnectProperties();
        effectiveWorkerConfig.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        this.connectorConfigs = connectConfig.getConnectors();
        this.config = new DistributedConfig(Utils.propsToStringMap(effectiveWorkerConfig));
        this.offsetBackingStore = new KafkaOffsetBackingStore();
        this.worker = new Worker(connectConfig.getWorkerId(), Time.SYSTEM, new Plugins(new HashMap<>()), config, offsetBackingStore);
        this.statusBackingStore = new KafkaStatusBackingStore(Time.SYSTEM, worker.getInternalValueConverter());
        this.configBackingStore = new KafkaConfigBackingStore(worker.getInternalValueConverter(), config, new WorkerConfigTransformer(worker, Collections.emptyMap()));
        this.herder = new DistributedHerder(config, Time.SYSTEM, worker, clusterId, statusBackingStore, configBackingStore, "");
    }

    @Override
    public void start() {

        offsetBackingStore.configure(config);
        statusBackingStore.configure(config);

        try {
            log.info("Embedded Kafka Connect is starting.");

            worker.start();
            herder.start();

            log.info("Embedded Kafka Connect started.");
            log.info("Found {} connectors to deploy.", connectorConfigs.size());

            connectorConfigs.forEach(this::deployConnector);
        } catch (Exception e) {
            throw new RuntimeException("Unable to start Embedded Kafka Connect.", e);
        }
    }

    private void deployConnector(final Properties connectorConfig) {
        final FutureCallback<Herder.Created<ConnectorInfo>> callback = new FutureCallback<>();
        final String connectorName = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
        log.info("Deploying connector {}.", connectorName);
        herder.putConnectorConfig(connectorName, Utils.propsToStringMap(connectorConfig), true, callback);
        try {
            callback.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to deploy connector {}.", connectorName, e);
        }
    }

    @Override
    public void stop() {
        try {
            final boolean wasShuttingDown = shutdown.getAndSet(true);
            if (!wasShuttingDown) {
                log.info("Embedded Kafka Connect is stopping.");
                herder.stop();
                worker.stop();
                log.info("Embedded Kafka Connect stopped.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to stop Embedded Kafka Connect.", e);
        }
    }
}
