package net.mguenther.kafka.browser.ui;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.KafkaBrowserService;
import net.mguenther.kafka.browser.service.TopicFormatDetector;
import net.mguenther.kafka.browser.service.TopicInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Left content panel showing the topic list with:
 * - Header with topic count, create button, and refresh button
 * - Filter text field for live-filtering
 * - List of topics with name, partition/replica info, and format badge
 * - Loading indicator during refresh
 * - Empty state message when no topics match
 */
public class TopicListPanel extends VBox {

    private KafkaBrowserService service;
    private final Consumer<String> onTopicSelected;

    private Label topicCountLabel;
    private TextField filterField;
    private ListView<TopicInfo> topicListView;
    private StackPane listContainer;
    private ProgressIndicator loadingIndicator;
    private Label emptyLabel;

    private List<TopicInfo> allTopics = new ArrayList<>();

    public TopicListPanel(KafkaBrowserService service, Consumer<String> onTopicSelected) {
        this.service = service;
        this.onTopicSelected = onTopicSelected;

        getStyleClass().add("topic-list-panel");
        setPadding(new Insets(15));
        setSpacing(10);

        buildUI();
    }

    private void buildUI() {
        // Header: "N TOPICS" + create + refresh
        HBox header = new HBox();
        header.setAlignment(Pos.CENTER_LEFT);
        header.setSpacing(10);

        topicCountLabel = new Label("0 TOPICS");
        topicCountLabel.getStyleClass().add("topic-count-label");
        HBox.setHgrow(topicCountLabel, Priority.ALWAYS);

        Button createTopicBtn = new Button("+");
        createTopicBtn.getStyleClass().add("create-topic-button");
        createTopicBtn.setOnAction(e -> showCreateTopicDialog());

        Button refreshBtn = new Button("\u21BB");
        refreshBtn.getStyleClass().add("refresh-button");
        refreshBtn.setOnAction(e -> refresh());

        header.getChildren().addAll(topicCountLabel, createTopicBtn, refreshBtn);

        // Filter field
        Label filterLabel = new Label("Filter topics:");
        filterLabel.getStyleClass().add("filter-label");

        filterField = new TextField();
        filterField.getStyleClass().add("filter-field");
        filterField.setPromptText("");
        filterField.textProperty().addListener((obs, oldVal, newVal) -> applyFilter(newVal));

        // Topic list in a StackPane so we can overlay loading/empty states
        topicListView = new ListView<>();
        topicListView.getStyleClass().add("topic-list-view");
        topicListView.setCellFactory(lv -> new TopicListCell());
        topicListView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                onTopicSelected.accept(newVal.getName());
            }
        });

        // Loading indicator
        loadingIndicator = new ProgressIndicator();
        loadingIndicator.getStyleClass().add("topic-loading-indicator");
        loadingIndicator.setMaxSize(40, 40);
        loadingIndicator.setVisible(false);

        // Empty state label
        emptyLabel = new Label("No topics in selection.");
        emptyLabel.getStyleClass().add("topic-empty-label");
        emptyLabel.setVisible(false);

        listContainer = new StackPane();
        listContainer.getChildren().addAll(topicListView, loadingIndicator, emptyLabel);
        VBox.setVgrow(listContainer, Priority.ALWAYS);

        getChildren().addAll(header, filterLabel, filterField, listContainer);
    }

    public void refresh() {
        // Show loading state
        loadingIndicator.setVisible(true);
        emptyLabel.setVisible(false);
        topicCountLabel.setText("\u21BB TOPICS");

        Thread thread = new Thread(() -> {
            List<TopicInfo> topics = service.listTopics();
            // Auto-detect formats
            TopicFormatDetector detector = new TopicFormatDetector(service.getBootstrapServers());
            for (TopicInfo topic : topics) {
                String format = detector.detect(topic.getName());
                topic.setFormat(format);
            }
            Platform.runLater(() -> {
                allTopics = topics;
                topicCountLabel.setText(topics.size() + (topics.size() == 1 ? " TOPIC" : " TOPICS"));
                loadingIndicator.setVisible(false);
                applyFilter(filterField.getText());
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void applyFilter(String filter) {
        String lowerFilter = filter != null ? filter.toLowerCase().trim() : "";
        List<TopicInfo> filtered;
        if (lowerFilter.isEmpty()) {
            filtered = allTopics;
        } else {
            filtered = new ArrayList<>();
            for (TopicInfo t : allTopics) {
                if (t.getName().toLowerCase().contains(lowerFilter)) {
                    filtered.add(t);
                }
            }
        }
        topicListView.getItems().setAll(filtered);

        // Show/hide empty state
        if (filtered.isEmpty() && !allTopics.isEmpty()) {
            emptyLabel.setText("No topics matching \u201C" + filter + "\u201D.");
            emptyLabel.setVisible(true);
        } else if (filtered.isEmpty() && allTopics.isEmpty()) {
            emptyLabel.setText("No topics in selection.");
            emptyLabel.setVisible(true);
        } else {
            emptyLabel.setVisible(false);
        }
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
    }

    private void showCreateTopicDialog() {
        CreateTopicDialog dialog = new CreateTopicDialog();
        dialog.setOnResult(topicConfig -> {
            if (topicConfig != null) {
                Thread thread = new Thread(() -> {
                    try {
                        service.createTopic(topicConfig);
                        Platform.runLater(this::refresh);
                    } catch (Exception ex) {
                        Platform.runLater(() -> {
                            // TODO: show error feedback
                        });
                    }
                });
                thread.setDaemon(true);
                thread.start();
            }
        });
        javafx.scene.Node node = this;
        while (node != null && !(node instanceof javafx.scene.layout.StackPane)) {
            node = node.getParent();
        }
        if (node instanceof javafx.scene.layout.StackPane sp) {
            dialog.showIn(sp);
        }
    }
}
