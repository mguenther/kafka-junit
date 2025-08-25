package net.mguenther.kafka.junit;

public final class KafkaConfigConstants {

    private KafkaConfigConstants() {
    }

    // Zookeeper Configuration Constants
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    public static final String ZOOKEEPER_SET_ACL = "zookeeper.set.acl";
    public static final String ZOOKEEPER_MAX_IN_FLIGHT_REQUESTS = "zookeeper.max.in.flight.requests";
    public static final String ZOOKEEPER_SSL_CLIENT_ENABLE = "zookeeper.ssl.client.enable";
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
    public static final String ZOOKEEPER_SSL_KEYSTORE_LOCATION = "zookeeper.ssl.keystore.location";
    public static final String ZOOKEEPER_SSL_KEYSTORE_PASSWORD = "zookeeper.ssl.keystore.password";
    public static final String ZOOKEEPER_SSL_KEYSTORE_TYPE = "zookeeper.ssl.keystore.type";
    public static final String ZOOKEEPER_SSL_TRUSTSTORE_LOCATION = "zookeeper.ssl.truststore.location";
    public static final String ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD = "zookeeper.ssl.truststore.password";
    public static final String ZOOKEEPER_SSL_TRUSTSTORE_TYPE = "zookeeper.ssl.truststore.type";
    public static final String ZOOKEEPER_SSL_PROTOCOL = "zookeeper.ssl.protocol";
    public static final String ZOOKEEPER_SSL_ENABLED_PROTOCOLS = "zookeeper.ssl.enabled.protocols";
    public static final String ZOOKEEPER_SSL_CIPHER_SUITES = "zookeeper.ssl.cipher.suites";
    public static final String ZOOKEEPER_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String ZOOKEEPER_SSL_CRL_ENABLE = "zookeeper.ssl.crl.enable";
    public static final String ZOOKEEPER_SSL_OCSP_ENABLE = "zookeeper.ssl.ocsp.enable";

    // Broker Configuration Constants
    public static final String BROKER_ID_GENERATION_ENABLE = "broker.id.generation.enable";
    public static final String RESERVED_BROKER_MAX_ID = "reserved.broker.max.id";
    public static final String BROKER_ID = "broker.id";
    public static final String MESSAGE_MAX_BYTES = "message.max.bytes";
    public static final String NUM_NETWORK_THREADS = "num.network.threads";
    public static final String NUM_IO_THREADS = "num.io.threads";
    public static final String NUM_REPLICA_ALTER_LOG_DIRS_THREADS = "num.replica.alter.log.dirs.threads";
    public static final String BACKGROUND_THREADS = "background.threads";
    public static final String QUEUED_MAX_REQUESTS = "queued.max.requests";
    public static final String QUEUED_MAX_REQUEST_BYTES = "queued.max.request.bytes";
    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS = "socket.connection.setup.timeout.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = "socket.connection.setup.timeout.max.ms";
    public static final String METADATA_LOG_MAX_RECORD_BYTES_BETWEEN_SNAPSHOTS = "metadata.log.max.record.bytes.between.snapshots";
    public static final String METADATA_LOG_MAX_SNAPSHOT_INTERVAL_MS = "metadata.log.max.snapshot.interval.ms";
    public static final String PROCESS_ROLES = "process.roles";
    public static final String NODE_ID = "node.id";
    public static final String INITIAL_BROKER_REGISTRATION_TIMEOUT_MS = "initial.broker.registration.timeout.ms";
    public static final String BROKER_HEARTBEAT_INTERVAL_MS = "broker.heartbeat.interval.ms";
    public static final String BROKER_SESSION_TIMEOUT_MS = "broker.session.timeout.ms";
    public static final String CONTROLLER_LISTENER_NAMES = "controller.listener.names";
    public static final String SASL_MECHANISM_CONTROLLER_PROTOCOL = "sasl.mechanism.controller.protocol";
    public static final String METADATA_LOG_DIR = "metadata.log.dir";
    public static final String METADATA_LOG_SEGMENT_BYTES = "metadata.log.segment.bytes";
    public static final String METADATA_LOG_SEGMENT_MIN_BYTES = "metadata.log.segment.min.bytes";
    public static final String METADATA_LOG_SEGMENT_MS = "metadata.log.segment.ms";
    public static final String METADATA_MAX_RETENTION_BYTES = "metadata.max.retention.bytes";
    public static final String METADATA_MAX_RETENTION_MS = "metadata.max.retention.ms";
    public static final String METADATA_MAX_IDLE_INTERVAL_MS = "metadata.max.idle.interval.ms";
    public static final String SERVER_MAX_STARTUP_TIME_MS = "server.max.startup.time.ms";
    public static final String ZOOKEEPER_METADATA_MIGRATION_ENABLE = "zookeeper.metadata.migration.enable";
    public static final String ELIGIBLE_LEADER_REPLICAS_ENABLE = "eligible.leader.replicas.enable";
    public static final String ZOOKEEPER_METADATA_MIGRATION_MIN_BATCH_SIZE = "zookeeper.metadata.migration.min.batch.size";
    public static final String AUTHORIZER_CLASS_NAME = "authorizer.class.name";
    public static final String EARLY_START_LISTENERS = "early.start.listeners";
    public static final String LISTENERS = "listeners";
    public static final String ADVERTISED_LISTENERS = "advertised.listeners";
    public static final String LISTENER_SECURITY_PROTOCOL_MAP = "listener.security.protocol.map";
    public static final String CONTROL_PLANE_LISTENER_NAME = "control.plane.listener.name";
    public static final String SOCKET_SEND_BUFFER_BYTES = "socket.send.buffer.bytes";
    public static final String SOCKET_RECEIVE_BUFFER_BYTES = "socket.receive.buffer.bytes";
    public static final String SOCKET_REQUEST_MAX_BYTES = "socket.request.max.bytes";
    public static final String SOCKET_LISTEN_BACKLOG_SIZE = "socket.listen.backlog.size";
    public static final String MAX_CONNECTIONS_PER_IP = "max.connections.per.ip";
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES = "max.connections.per.ip.overrides";
    public static final String MAX_CONNECTIONS = "max.connections";
    public static final String MAX_CONNECTION_CREATION_RATE = "max.connection.creation.rate";
    public static final String CONNECTIONS_MAX_IDLE_MS = "connections.max.idle.ms";
    public static final String CONNECTION_FAILED_AUTHENTICATION_DELAY_MS = "connection.failed.authentication.delay.ms";
    public static final String BROKER_RACK = "broker.rack";

    // Log Configuration Constants
    public static final String NUM_PARTITIONS = "num.partitions";
    public static final String LOG_DIR = "log.dir";
    public static final String LOG_DIRS = "log.dirs";
    public static final String LOG_SEGMENT_BYTES = "log.segment.bytes";
    public static final String LOG_ROLL_MS = "log.roll.ms";
    public static final String LOG_ROLL_HOURS = "log.roll.hours";
    public static final String LOG_ROLL_JITTER_MS = "log.roll.jitter.ms";
    public static final String LOG_ROLL_JITTER_HOURS = "log.roll.jitter.hours";
    public static final String LOG_RETENTION_MS = "log.retention.ms";
    public static final String LOG_RETENTION_MINUTES = "log.retention.minutes";
    public static final String LOG_RETENTION_HOURS = "log.retention.hours";
    public static final String LOG_RETENTION_BYTES = "log.retention.bytes";
    public static final String LOG_RETENTION_CHECK_INTERVAL_MS = "log.retention.check.interval.ms";
    public static final String LOG_CLEANUP_POLICY = "log.cleanup.policy";
    public static final String LOG_CLEANER_THREADS = "log.cleaner.threads";
    public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND = "log.cleaner.io.max.bytes.per.second";
    public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE = "log.cleaner.dedupe.buffer.size";
    public static final String LOG_CLEANER_IO_BUFFER_SIZE = "log.cleaner.io.buffer.size";
    public static final String LOG_CLEANER_IO_BUFFER_LOAD_FACTOR = "log.cleaner.io.buffer.load.factor";
    public static final String LOG_CLEANER_BACKOFF_MS = "log.cleaner.backoff.ms";
    public static final String LOG_CLEANER_MIN_CLEANABLE_RATIO = "log.cleaner.min.cleanable.ratio";
    public static final String LOG_CLEANER_ENABLE = "log.cleaner.enable";
    public static final String LOG_CLEANER_DELETE_RETENTION_MS = "log.cleaner.delete.retention.ms";
    public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS = "log.cleaner.min.compaction.lag.ms";
    public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS = "log.cleaner.max.compaction.lag.ms";
    public static final String LOG_INDEX_SIZE_MAX_BYTES = "log.index.size.max.bytes";
    public static final String LOG_INDEX_INTERVAL_BYTES = "log.index.interval.bytes";
    public static final String LOG_FLUSH_INTERVAL_MESSAGES = "log.flush.interval.messages";
    public static final String LOG_SEGMENT_DELETE_DELAY_MS = "log.segment.delete.delay.ms";
    public static final String LOG_FLUSH_SCHEDULER_INTERVAL_MS = "log.flush.scheduler.interval.ms";
    public static final String LOG_FLUSH_INTERVAL_MS = "log.flush.interval.ms";
    public static final String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS = "log.flush.offset.checkpoint.interval.ms";
    public static final String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS = "log.flush.start.offset.checkpoint.interval.ms";
    public static final String LOG_PREALLOCATE = "log.preallocate";
    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR = "num.recovery.threads.per.data.dir";
    public static final String AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable";
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";
    public static final String LOG_MESSAGE_TIMESTAMP_TYPE = "log.message.timestamp.type";
    public static final String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS = "log.message.timestamp.difference.max.ms";
    public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS = "log.message.timestamp.before.max.ms";
    public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS = "log.message.timestamp.after.max.ms";
    public static final String CREATE_TOPIC_POLICY_CLASS_NAME = "create.topic.policy.class.name";
    public static final String ALTER_CONFIG_POLICY_CLASS_NAME = "alter.config.policy.class.name";
    public static final String LOG_MESSAGE_DOWNCONVERSION_ENABLE = "log.message.downconversion.enable";
    public static final String CONTROLLER_SOCKET_TIMEOUT_MS = "controller.socket.timeout.ms";
    public static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";
    public static final String REPLICA_LAG_TIME_MAX_MS = "replica.lag.time.max.ms";
    public static final String REPLICA_SOCKET_TIMEOUT_MS = "replica.socket.timeout.ms";
    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES = "replica.socket.receive.buffer.bytes";
    public static final String REPLICA_FETCH_MAX_BYTES = "replica.fetch.max.bytes";
    public static final String REPLICA_FETCH_WAIT_MAX_MS = "replica.fetch.wait.max.ms";
    public static final String REPLICA_FETCH_BACKOFF_MS = "replica.fetch.backoff.ms";
    public static final String REPLICA_FETCH_MIN_BYTES = "replica.fetch.min.bytes";
    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES = "replica.fetch.response.max.bytes";
    public static final String NUM_REPLICA_FETCHERS = "num.replica.fetchers";
    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS = "replica.high.watermark.checkpoint.interval.ms";
    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS = "fetch.purgatory.purge.interval.requests";
    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS = "producer.purgatory.purge.interval.requests";
    public static final String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS = "delete.records.purgatory.purge.interval.requests";
    public static final String AUTO_LEADER_REBALANCE_ENABLE = "auto.leader.rebalance.enable";
    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE = "leader.imbalance.per.broker.percentage";
    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS = "leader.imbalance.check.interval.seconds";
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE = "unclean.leader.election.enable";
    public static final String SECURITY_INTER_BROKER_PROTOCOL = "security.inter.broker.protocol";
    public static final String INTER_BROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";
    public static final String INTER_BROKER_LISTENER_NAME = "inter.broker.listener.name";
    public static final String REPLICA_SELECTOR_CLASS = "replica.selector.class";
    public static final String CONTROLLED_SHUTDOWN_MAX_RETRIES = "controlled.shutdown.max.retries";
    public static final String CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS = "controlled.shutdown.retry.backoff.ms";
    public static final String CONTROLLED_SHUTDOWN_ENABLE = "controlled.shutdown.enable";

    // Group Coordinator Configuration Constants
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS = "group.min.session.timeout.ms";
    public static final String GROUP_MAX_SESSION_TIMEOUT_MS = "group.max.session.timeout.ms";
    public static final String GROUP_INITIAL_REBALANCE_DELAY_MS = "group.initial.rebalance.delay.ms";
    public static final String GROUP_MAX_SIZE = "group.max.size";
    public static final String GROUP_COORDINATOR_REBALANCE_PROTOCOLS = "group.coordinator.rebalance.protocols";
    public static final String GROUP_COORDINATOR_THREADS = "group.coordinator.threads";
    public static final String GROUP_COORDINATOR_NEW_ENABLE = "group.coordinator.new.enable";
    public static final String GROUP_CONSUMER_SESSION_TIMEOUT_MS = "group.consumer.session.timeout.ms";
    public static final String GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS = "group.consumer.min.session.timeout.ms";
    public static final String GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS = "group.consumer.max.session.timeout.ms";
    public static final String GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS = "group.consumer.heartbeat.interval.ms";
    public static final String GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS = "group.consumer.min.heartbeat.interval.ms";
    public static final String GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS = "group.consumer.max.heartbeat.interval.ms";
    public static final String GROUP_CONSUMER_MAX_SIZE = "group.consumer.max.size";
    public static final String GROUP_CONSUMER_ASSIGNORS = "group.consumer.assignors";

    // Offset Management Constants
    public static final String OFFSET_METADATA_MAX_BYTES = "offset.metadata.max.bytes";
    public static final String OFFSETS_LOAD_BUFFER_SIZE = "offsets.load.buffer.size";
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR = "offsets.topic.replication.factor";
    public static final String OFFSETS_TOPIC_NUM_PARTITIONS = "offsets.topic.num.partitions";
    public static final String OFFSETS_TOPIC_SEGMENT_BYTES = "offsets.topic.segment.bytes";
    public static final String OFFSETS_TOPIC_COMPRESSION_CODEC = "offsets.topic.compression.codec";
    public static final String OFFSETS_RETENTION_MINUTES = "offsets.retention.minutes";
    public static final String OFFSETS_RETENTION_CHECK_INTERVAL_MS = "offsets.retention.check.interval.ms";
    public static final String OFFSETS_COMMIT_TIMEOUT_MS = "offsets.commit.timeout.ms";
    public static final String OFFSETS_COMMIT_REQUIRED_ACKS = "offsets.commit.required.acks";

    // Topic Management Constants
    public static final String DELETE_TOPIC_ENABLE = "delete.topic.enable";
    public static final String COMPRESSION_TYPE = "compression.type";

    // Transactional Configuration Constants
    public static final String TRANSACTIONAL_ID_EXPIRATION_MS = "transactional.id.expiration.ms";
    public static final String TRANSACTION_MAX_TIMEOUT_MS = "transaction.max.timeout.ms";
    public static final String TRANSACTION_STATE_LOG_MIN_ISR = "transaction.state.log.min.isr";
    public static final String TRANSACTION_STATE_LOG_LOAD_BUFFER_SIZE = "transaction.state.log.load.buffer.size";
    public static final String TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "transaction.state.log.replication.factor";
    public static final String TRANSACTION_STATE_LOG_NUM_PARTITIONS = "transaction.state.log.num.partitions";
    public static final String TRANSACTION_STATE_LOG_SEGMENT_BYTES = "transaction.state.log.segment.bytes";
    public static final String TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS = "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    public static final String TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS = "transaction.remove.expired.transaction.cleanup.interval.ms";
    public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE = "transaction.partition.verification.enable";
    public static final String PRODUCER_ID_EXPIRATION_MS = "producer.id.expiration.ms";
    public static final String PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS = "producer.id.expiration.check.interval.ms";

    // Fetch and Metrics Configuration Constants
    public static final String MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS = "max.incremental.fetch.session.cache.slots";
    public static final String FETCH_MAX_BYTES = "fetch.max.bytes";
    public static final String METRICS_NUM_SAMPLES = "metrics.num.samples";
    public static final String METRICS_SAMPLE_WINDOW_MS = "metrics.sample.window.ms";
    public static final String METRIC_REPORTERS = "metric.reporters";
    public static final String METRICS_RECORDING_LEVEL = "metrics.recording.level";
    public static final String AUTO_INCLUDE_JMX_REPORTER = "auto.include.jmx.reporter";
    public static final String KAFKA_METRICS_REPORTERS = "kafka.metrics.reporters";
    public static final String KAFKA_METRICS_POLLING_INTERVAL_SECS = "kafka.metrics.polling.interval.secs";
    public static final String TELEMETRY_MAX_BYTES = "telemetry.max.bytes";

    // Quota Configuration Constants
    public static final String QUOTA_WINDOW_NUM = "quota.window.num";
    public static final String REPLICATION_QUOTA_WINDOW_NUM = "replication.quota.window.num";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_NUM = "alter.log.dirs.replication.quota.window.num";
    public static final String CONTROLLER_QUOTA_WINDOW_NUM = "controller.quota.window.num";
    public static final String QUOTA_WINDOW_SIZE_SECONDS = "quota.window.size.seconds";
    public static final String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS = "replication.quota.window.size.seconds";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS = "alter.log.dirs.replication.quota.window.size.seconds";
    public static final String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS = "controller.quota.window.size.seconds";
    public static final String CLIENT_QUOTA_CALLBACK_CLASS = "client.quota.callback.class";

    // Security Configuration Constants
    public static final String CONNECTIONS_MAX_REAUTH_MS = "connections.max.reauth.ms";
    public static final String SASL_SERVER_MAX_RECEIVE_SIZE = "sasl.server.max.receive.size";
    public static final String SECURITY_PROVIDERS = "security.providers";
    public static final String PRINCIPAL_BUILDER_CLASS = "principal.builder.class";
    public static final String SSL_PROTOCOL = "ssl.protocol";
    public static final String SSL_PROVIDER = "ssl.provider";
    public static final String SSL_ENABLED_PROTOCOLS = "ssl.enabled.protocols";
    public static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
    public static final String SSL_KEY_PASSWORD = "ssl.key.password";
    public static final String SSL_KEYSTORE_KEY = "ssl.keystore.key";
    public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN = "ssl.keystore.certificate.chain";
    public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_CERTIFICATES = "ssl.truststore.certificates";
    public static final String SSL_KEYMANAGER_ALGORITHM = "ssl.keymanager.algorithm";
    public static final String SSL_TRUSTMANAGER_ALGORITHM = "ssl.trustmanager.algorithm";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "ssl.endpoint.identification.algorithm";
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION = "ssl.secure.random.implementation";
    public static final String SSL_CLIENT_AUTH = "ssl.client.auth";
    public static final String SSL_CIPHER_SUITES = "ssl.cipher.suites";
    public static final String SSL_PRINCIPAL_MAPPING_RULES = "ssl.principal.mapping.rules";
    public static final String SSL_ENGINE_FACTORY_CLASS = "ssl.engine.factory.class";
    public static final String SSL_ALLOW_DN_CHANGES = "ssl.allow.dn.changes";
    public static final String SSL_ALLOW_SAN_CHANGES = "ssl.allow.san.changes";
    public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL = "sasl.mechanism.inter.broker.protocol";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_ENABLED_MECHANISMS = "sasl.enabled.mechanisms";
    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS = "sasl.server.callback.handler.class";
    public static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";
    public static final String SASL_LOGIN_CLASS = "sasl.login.class";
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String SASL_KERBEROS_KINIT_CMD = "sasl.kerberos.kinit.cmd";
    public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR = "sasl.kerberos.ticket.renew.window.factor";
    public static final String SASL_KERBEROS_TICKET_RENEW_JITTER = "sasl.kerberos.ticket.renew.jitter";
    public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN = "sasl.kerberos.min.time.before.relogin";
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = "sasl.kerberos.principal.to.local.rules";
    public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR = "sasl.login.refresh.window.factor";
    public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER = "sasl.login.refresh.window.jitter";
    public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS = "sasl.login.refresh.min.period.seconds";
    public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS = "sasl.login.refresh.buffer.seconds";
    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS = "sasl.login.connect.timeout.ms";
    public static final String SASL_LOGIN_READ_TIMEOUT_MS = "sasl.login.read.timeout.ms";
    public static final String SASL_LOGIN_RETRY_BACKOFF_MAX_MS = "sasl.login.retry.backoff.max.ms";
    public static final String SASL_LOGIN_RETRY_BACKOFF_MS = "sasl.login.retry.backoff.ms";
    public static final String SASL_OAUTHBEARER_SCOPE_CLAIM_NAME = "sasl.oauthbearer.scope.claim.name";
    public static final String SASL_OAUTHBEARER_SUB_CLAIM_NAME = "sasl.oauthbearer.sub.claim.name";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS = "sasl.oauthbearer.jwks.endpoint.refresh.ms";
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS = "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms";
    public static final String SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS = "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms";
    public static final String SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS = "sasl.oauthbearer.clock.skew.seconds";
    public static final String SASL_OAUTHBEARER_EXPECTED_AUDIENCE = "sasl.oauthbearer.expected.audience";
    public static final String SASL_OAUTHBEARER_EXPECTED_ISSUER = "sasl.oauthbearer.expected.issuer";
    public static final String DELEGATION_TOKEN_MASTER_KEY = "delegation.token.master.key";
    public static final String DELEGATION_TOKEN_SECRET_KEY = "delegation.token.secret.key";
    public static final String DELEGATION_TOKEN_MAX_LIFETIME_MS = "delegation.token.max.lifetime.ms";
    public static final String DELEGATION_TOKEN_EXPIRY_TIME_MS = "delegation.token.expiry.time.ms";
    public static final String DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS = "delegation.token.expiry.check.interval.ms";
    public static final String PASSWORD_ENCODER_SECRET = "password.encoder.secret";
    public static final String PASSWORD_ENCODER_OLD_SECRET = "password.encoder.old.secret";
    public static final String PASSWORD_ENCODER_KEYFACTORY_ALGORITHM = "password.encoder.keyfactory.algorithm";
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM = "password.encoder.cipher.algorithm";
    public static final String PASSWORD_ENCODER_KEY_LENGTH = "password.encoder.key.length";
    public static final String PASSWORD_ENCODER_ITERATIONS = "password.encoder.iterations";

    // Controller Quorum Configuration Constants
    public static final String CONTROLLER_QUORUM_VOTERS = "controller.quorum.voters";
    public static final String CONTROLLER_QUORUM_ELECTION_TIMEOUT_MS = "controller.quorum.election.timeout.ms";
    public static final String CONTROLLER_QUORUM_FETCH_TIMEOUT_MS = "controller.quorum.fetch.timeout.ms";
    public static final String CONTROLLER_QUORUM_ELECTION_BACKOFF_MAX_MS = "controller.quorum.election.backoff.max.ms";
    public static final String CONTROLLER_QUORUM_APPEND_LINGER_MS = "controller.quorum.append.linger.ms";
    public static final String CONTROLLER_QUORUM_REQUEST_TIMEOUT_MS = "controller.quorum.request.timeout.ms";
    public static final String CONTROLLER_QUORUM_RETRY_BACKOFF_MS = "controller.quorum.retry.backoff.ms";

    // Unstable API/Metadata Versions Configuration Constants
    public static final String UNSTABLE_API_VERSIONS_ENABLE = "unstable.api.versions.enable";
    public static final String UNSTABLE_METADATA_VERSIONS_ENABLE = "unstable.metadata.versions.enable";

    // Remote Log Storage Configuration Constants
    public static final String REMOTE_LOG_STORAGE_SYSTEM_ENABLE = "remote.log.storage.system.enable";
    public static final String REMOTE_LOG_STORAGE_MANAGER_IMPL_PREFIX = "remote.log.storage.manager.impl.prefix";
    public static final String REMOTE_LOG_METADATA_MANAGER_IMPL_PREFIX = "remote.log.metadata.manager.impl.prefix";
    public static final String REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME = "remote.log.storage.manager.class.name";
    public static final String REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH = "remote.log.storage.manager.class.path";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_NAME = "remote.log.metadata.manager.class.name";
    public static final String REMOTE_LOG_METADATA_MANAGER_CLASS_PATH = "remote.log.metadata.manager.class.path";
    public static final String REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME = "remote.log.metadata.manager.listener.name";
    public static final String REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES = "remote.log.metadata.custom.metadata.max.bytes";
    public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES = "remote.log.index.file.cache.total.size.bytes";
    public static final String REMOTE_LOG_MANAGER_THREAD_POOL_SIZE = "remote.log.manager.thread.pool.size";
    public static final String REMOTE_LOG_MANAGER_TASK_INTERVAL_MS = "remote.log.manager.task.interval.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACKOFF_MS = "remote.log.manager.task.retry.backoff.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_BACKOFF_MAX_MS = "remote.log.manager.task.retry.backoff.max.ms";
    public static final String REMOTE_LOG_MANAGER_TASK_RETRY_JITTER = "remote.log.manager.task.retry.jitter";
    public static final String REMOTE_LOG_READER_THREADS = "remote.log.reader.threads";
    public static final String REMOTE_LOG_READER_MAX_PENDING_TASKS = "remote.log.reader.max.pending.tasks";
    public static final String LOG_LOCAL_RETENTION_MS = "log.local.retention.ms";
    public static final String LOG_LOCAL_RETENTION_BYTES = "log.local.retention.bytes";
}
