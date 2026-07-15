package net.mguenther.kafka.browser.ui;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.KafkaBrowserService;
import net.mguenther.kafka.junit.KeyValue;

import java.util.List;

/**
 * Search view for records in the selected topic. Shows results as expandable cards
 * with key/value expand triangles, offset, and partition info.
 */
public class RecordSearchView extends VBox {

    private static final int SEARCH_LIMIT = 100;

    private KafkaBrowserService service;
    private String currentTopic;

    private Label topicHeaderLabel;
    private TextField searchField;
    private Label resultCountLabel;
    private VBox resultsContainer;

    public RecordSearchView(KafkaBrowserService service) {
        this.service = service;
        getStyleClass().add("record-search-view");
        setPadding(new Insets(15));
        setSpacing(10);
        buildUI();
    }

    private void buildUI() {
        // Header
        HBox headerRow = new HBox();
        headerRow.setAlignment(Pos.CENTER_LEFT);
        headerRow.getStyleClass().add("record-header");

        topicHeaderLabel = new Label("");
        topicHeaderLabel.getStyleClass().add("record-topic-header");
        HBox.setHgrow(topicHeaderLabel, Priority.ALWAYS);

        Button settingsBtn = new Button("\uD83D\uDD27");
        settingsBtn.getStyleClass().add("settings-button");

        headerRow.getChildren().addAll(topicHeaderLabel, settingsBtn);

        // Search bar
        HBox searchRow = new HBox(10);
        searchRow.setAlignment(Pos.CENTER_LEFT);

        searchField = new TextField();
        searchField.setPromptText("");
        searchField.getStyleClass().add("search-field");
        HBox.setHgrow(searchField, Priority.ALWAYS);
        searchField.setOnAction(e -> performSearch());

        Button searchBtn = new Button("Search");
        searchBtn.getStyleClass().add("search-button");
        searchBtn.setOnAction(e -> performSearch());
        searchBtn.disableProperty().bind(searchField.textProperty().isEmpty());

        searchRow.getChildren().addAll(searchField, searchBtn);

        // Result count
        resultCountLabel = new Label("");
        resultCountLabel.getStyleClass().add("showing-label");
        resultCountLabel.setAlignment(Pos.CENTER_RIGHT);
        resultCountLabel.setMaxWidth(Double.MAX_VALUE);
        HBox countRow = new HBox(resultCountLabel);
        countRow.setAlignment(Pos.CENTER_RIGHT);

        // Results scroll area
        resultsContainer = new VBox(8);
        resultsContainer.getStyleClass().add("search-results-container");

        ScrollPane scrollPane = new ScrollPane(resultsContainer);
        scrollPane.setFitToWidth(true);
        scrollPane.getStyleClass().add("search-scroll-pane");
        VBox.setVgrow(scrollPane, Priority.ALWAYS);

        getChildren().addAll(headerRow, searchRow, countRow, scrollPane);
    }

    public void setTopic(String topicName) {
        this.currentTopic = topicName;
        topicHeaderLabel.setText(topicName);
        resultsContainer.getChildren().clear();
        resultCountLabel.setText("");
    }

    private void performSearch() {
        String searchTerm = searchField.getText().trim();
        if (searchTerm.isEmpty() || currentTopic == null) return;

        resultsContainer.getChildren().clear();
        resultCountLabel.setText("Searching...");

        Thread thread = new Thread(() -> {
            List<KeyValue<String, String>> results = service.searchRecords(currentTopic, searchTerm, SEARCH_LIMIT);
            // Get total count for info display
            int totalFound = results.size();
            Platform.runLater(() -> {
                resultCountLabel.setText("Showing " + totalFound + " out of " + totalFound + (totalFound == 1 ? " record." : " records."));
                for (KeyValue<String, String> kv : results) {
                    resultsContainer.getChildren().add(new SearchResultCard(kv));
                }
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
    }
}
