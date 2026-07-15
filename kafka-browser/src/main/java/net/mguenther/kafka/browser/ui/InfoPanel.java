package net.mguenther.kafka.browser.ui;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Separator;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;
import net.mguenther.kafka.browser.service.KafkaBrowserService;
import net.mguenther.kafka.browser.service.TopicInfo;
import net.mguenther.kafka.junit.LeaderAndIsr;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Info panel shown when the info tab is selected. Takes full width (no topic list).
 * Displays:
 * - Connection status (workspace, environment, health)
 * - Cluster information (ID, brokers, controller)
 * - All topics with expandable configuration details
 */
public class InfoPanel extends VBox {

    private final KafkaBrowserService service;
    private final BrowserConfig config;

    private VBox connectionSection;
    private VBox clusterSection;
    private VBox topicsSection;
    private VBox topicsListContainer;

    public InfoPanel(KafkaBrowserService service, BrowserConfig config) {
        this.service = service;
        this.config = config;

        getStyleClass().add("info-panel");
        setPadding(new Insets(20));
        setSpacing(0);

        ScrollPane scrollPane = new ScrollPane();
        scrollPane.setFitToWidth(true);
        scrollPane.getStyleClass().add("info-scroll-pane");
        VBox.setVgrow(scrollPane, Priority.ALWAYS);

        VBox content = new VBox(20);
        content.setPadding(new Insets(0, 10, 0, 0));

        // Connection status section
        connectionSection = createSection("Connection");
        content.getChildren().add(connectionSection);

        // Cluster info section
        clusterSection = createSection("Cluster");
        content.getChildren().add(clusterSection);

        // Topics section
        topicsSection = createSection("Topics");
        topicsListContainer = new VBox(4);
        topicsSection.getChildren().add(topicsListContainer);
        content.getChildren().add(topicsSection);

        scrollPane.setContent(content);
        getChildren().add(scrollPane);
    }

    private VBox createSection(String title) {
        VBox section = new VBox(8);
        section.getStyleClass().add("info-section");

        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("info-section-title");

        Separator sep = new Separator();
        sep.getStyleClass().add("info-separator");

        section.getChildren().addAll(titleLabel, sep);
        return section;
    }

    public void loadInfo() {
        populateConnectionSection();

        // Cluster info + topics (async)
        Thread thread = new Thread(() -> {
            KafkaBrowserService.ClusterInfo info = service.getClusterInfo();
            List<TopicInfo> topics = service.listTopics();
            Platform.runLater(() -> {
                populateClusterSection(info);
                populateTopicsSection(topics);
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void populateConnectionSection() {
        clearSection(connectionSection);

        Workspace ws = config.getActiveWorkspace();
        Environment env = config.getActiveEnvironment();

        addInfoRow(connectionSection, "Workspace", ws != null ? ws.getName() : "None");
        addInfoRow(connectionSection, "Bootstrap Servers", ws != null ? ws.getBootstrapServers() : "\u2014");
        addInfoRow(connectionSection, "Environment", env != null ? env.getName() : "Global");

        if (env != null && !env.getParameters().isEmpty()) {
            addInfoRow(connectionSection, "Parameters", env.getParameters().size() + " overrides");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        Label statusLabel = new Label("\u2713 Connected at " + timestamp);
        statusLabel.getStyleClass().add("info-status-connected");
        connectionSection.getChildren().add(statusLabel);
    }

    private void populateClusterSection(KafkaBrowserService.ClusterInfo info) {
        clearSection(clusterSection);

        addInfoRow(clusterSection, "Cluster ID", info.clusterId());
        addInfoRow(clusterSection, "Kafka Version", info.kafkaVersion());
        addInfoRow(clusterSection, "Controller", info.controller());
        addInfoRow(clusterSection, "Brokers", String.valueOf(info.brokers().size()));

        for (String broker : info.brokers()) {
            Label brokerLabel = new Label("  " + broker);
            brokerLabel.getStyleClass().add("info-value-detail");
            clusterSection.getChildren().add(brokerLabel);
        }

        addInfoRow(clusterSection, "Total Topics", String.valueOf(info.totalTopics()));
    }

    private void populateTopicsSection(List<TopicInfo> topics) {
        topicsListContainer.getChildren().clear();

        if (topics.isEmpty()) {
            Label empty = new Label("No topics found.");
            empty.getStyleClass().add("info-value");
            topicsListContainer.getChildren().add(empty);
            return;
        }

        for (TopicInfo topic : topics) {
            topicsListContainer.getChildren().add(new ExpandableTopicRow(topic));
        }
    }

    private void addInfoRow(VBox section, String label, String value) {
        HBox row = new HBox(10);
        row.setAlignment(Pos.CENTER_LEFT);

        Label keyLabel = new Label(label + ":");
        keyLabel.getStyleClass().add("info-key");
        keyLabel.setMinWidth(130);

        Label valueLabel = new Label(value);
        valueLabel.getStyleClass().add("info-value");
        HBox.setHgrow(valueLabel, Priority.ALWAYS);

        row.getChildren().addAll(keyLabel, valueLabel);
        section.getChildren().add(row);
    }

    private void clearSection(VBox section) {
        while (section.getChildren().size() > 2) {
            section.getChildren().remove(2);
        }
    }

    /**
     * An expandable row for a single topic. Shows name + partition/replica summary.
     * Click to expand and load full configuration details.
     */
    private class ExpandableTopicRow extends VBox {

        private boolean expanded = false;
        private VBox detailsContainer;
        private boolean detailsLoaded = false;
        private final TopicInfo topic;

        ExpandableTopicRow(TopicInfo topic) {
            this.topic = topic;
            setSpacing(0);
            getStyleClass().add("info-topic-row");

            // Header row (always visible)
            HBox header = new HBox(10);
            header.setAlignment(Pos.CENTER_LEFT);
            header.setPadding(new Insets(6, 8, 6, 8));
            header.setCursor(javafx.scene.Cursor.HAND);
            header.getStyleClass().add("info-topic-header");

            Label triangle = new Label("\u25B8");
            triangle.getStyleClass().add("expand-triangle");

            Label nameLabel = new Label(topic.getName());
            nameLabel.getStyleClass().add("info-topic-name");
            HBox.setHgrow(nameLabel, Priority.ALWAYS);

            Label metaLabel = new Label(
                    topic.getPartitions() + (topic.getPartitions() == 1 ? " partition" : " partitions")
                            + " x " + topic.getReplicas() + (topic.getReplicas() == 1 ? " replica" : " replicas"));
            metaLabel.getStyleClass().add("info-topic-meta");

            header.getChildren().addAll(triangle, nameLabel, metaLabel);

            // Details container (hidden until expanded)
            detailsContainer = new VBox(4);
            detailsContainer.setPadding(new Insets(4, 8, 8, 28));
            detailsContainer.getStyleClass().add("info-topic-details");
            detailsContainer.setVisible(false);
            detailsContainer.setManaged(false);

            header.setOnMouseClicked(e -> {
                expanded = !expanded;
                triangle.setText(expanded ? "\u25BE" : "\u25B8");
                detailsContainer.setVisible(expanded);
                detailsContainer.setManaged(expanded);
                if (expanded && !detailsLoaded) {
                    loadDetails();
                }
            });

            getChildren().addAll(header, detailsContainer);
        }

        private void loadDetails() {
            Label loadingLabel = new Label("Loading...");
            loadingLabel.getStyleClass().add("info-value");
            detailsContainer.getChildren().add(loadingLabel);

            Thread thread = new Thread(() -> {
                KafkaBrowserService.TopicDetails details = service.fetchTopicDetails(topic.getName());
                Platform.runLater(() -> {
                    detailsLoaded = true;
                    detailsContainer.getChildren().clear();

                    // Partition info
                    addDetailRow("Partitions", String.valueOf(details.partitions().size()));
                    addDetailRow("Messages (approx.)", String.valueOf(details.approximateMessageCount()));

                    for (Map.Entry<Integer, LeaderAndIsr> entry : details.partitions().entrySet()) {
                        LeaderAndIsr lai = entry.getValue();
                        Label partDetail = new Label(
                                "  P" + entry.getKey() + "  leader=" + lai.getLeader() + "  ISR=" + lai.getIsr());
                        partDetail.getStyleClass().add("info-value-detail");
                        detailsContainer.getChildren().add(partDetail);
                    }

                    // Configuration
                    Properties config = details.config();
                    if (!config.isEmpty()) {
                        Label configTitle = new Label("Configuration:");
                        configTitle.getStyleClass().add("info-subsection-title");
                        configTitle.setPadding(new Insets(6, 0, 2, 0));
                        detailsContainer.getChildren().add(configTitle);

                        config.stringPropertyNames().stream().sorted().forEach(key -> {
                            Label cfgLine = new Label("  " + key + " = " + config.getProperty(key));
                            cfgLine.getStyleClass().add("info-value-detail");
                            detailsContainer.getChildren().add(cfgLine);
                        });
                    }
                });
            });
            thread.setDaemon(true);
            thread.start();
        }

        private void addDetailRow(String label, String value) {
            HBox row = new HBox(8);
            row.setAlignment(Pos.CENTER_LEFT);
            Label keyLabel = new Label(label + ":");
            keyLabel.getStyleClass().add("info-key");
            keyLabel.setMinWidth(120);
            Label valueLabel = new Label(value);
            valueLabel.getStyleClass().add("info-value");
            row.getChildren().addAll(keyLabel, valueLabel);
            detailsContainer.getChildren().add(row);
        }
    }
}
