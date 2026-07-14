package net.mguenther.kafka.browser.ui;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.KafkaBrowserService;
import net.mguenther.kafka.browser.service.TopicFormatDetector;
import net.mguenther.kafka.browser.service.TopicInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Left content panel showing the topic list with:
 * - Header with topic count and refresh button
 * - Filter text field for live-filtering
 * - List of topics with name, partition/replica info, and format badge
 */
public class TopicListPanel extends VBox {

    private KafkaBrowserService service;
    private final Consumer<String> onTopicSelected;

    private Label topicCountLabel;
    private TextField filterField;
    private ListView<TopicInfo> topicListView;

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
        // Header: "N TOPICS" + refresh button
        HBox header = new HBox();
        header.setAlignment(Pos.CENTER_LEFT);
        header.setSpacing(10);

        topicCountLabel = new Label("0 TOPICS");
        topicCountLabel.getStyleClass().add("topic-count-label");
        HBox.setHgrow(topicCountLabel, Priority.ALWAYS);

        Button refreshBtn = new Button("\u21BB");
        refreshBtn.getStyleClass().add("refresh-button");
        refreshBtn.setOnAction(e -> refresh());

        header.getChildren().addAll(topicCountLabel, refreshBtn);

        // Filter field
        Label filterLabel = new Label("Filter topics:");
        filterLabel.getStyleClass().add("filter-label");

        filterField = new TextField();
        filterField.getStyleClass().add("filter-field");
        filterField.setPromptText("");
        filterField.textProperty().addListener((obs, oldVal, newVal) -> applyFilter(newVal));

        // Topic list
        topicListView = new ListView<>();
        topicListView.getStyleClass().add("topic-list-view");
        topicListView.setCellFactory(lv -> new TopicListCell());
        topicListView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                onTopicSelected.accept(newVal.getName());
            }
        });
        VBox.setVgrow(topicListView, Priority.ALWAYS);

        getChildren().addAll(header, filterLabel, filterField, topicListView);
    }

    public void refresh() {
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
                topicCountLabel.setText(topics.size() + " TOPICS");
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
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
    }
}
