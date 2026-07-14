package net.mguenther.kafka.browser.ui;

import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.KafkaBrowserService;
import net.mguenther.kafka.junit.KeyValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Record table view showing Kafka records in a table format with:
 * - Topic name header + wrench icon
 * - Filter field, partition selector, seek-to-offset
 * - Table with Offset, Key, Value columns
 * - Pagination buttons (first, prev, next, last) + produce button
 */
public class RecordTableView extends VBox {

    private static final int PAGE_SIZE = 12;

    private KafkaBrowserService service;
    private String currentTopic;
    private int currentPartition = 0;
    private long currentOffset = 0;
    private long endOffset = 0;
    private long beginOffset = 0;

    private Label topicHeaderLabel;
    private Label showingLabel;
    private ComboBox<String> partitionSelector;
    private TextField seekField;
    private TextField filterField;
    private TableView<KeyValue<String, String>> table;

    public RecordTableView(KafkaBrowserService service) {
        this.service = service;
        getStyleClass().add("record-table-view");
        setPadding(new Insets(15));
        setSpacing(10);
        buildUI();
    }

    private void buildUI() {
        // Header row: topic name + settings icon
        HBox headerRow = new HBox();
        headerRow.setAlignment(Pos.CENTER_LEFT);
        headerRow.getStyleClass().add("record-header");

        topicHeaderLabel = new Label("");
        topicHeaderLabel.getStyleClass().add("record-topic-header");
        HBox.setHgrow(topicHeaderLabel, Priority.ALWAYS);

        Button settingsBtn = new Button("\uD83D\uDD27");
        settingsBtn.getStyleClass().add("settings-button");

        headerRow.getChildren().addAll(topicHeaderLabel, settingsBtn);

        // Controls row: filter, partition, seek
        HBox controlsRow = new HBox(10);
        controlsRow.setAlignment(Pos.CENTER_LEFT);

        filterField = new TextField();
        filterField.setPromptText("filter");
        filterField.getStyleClass().add("record-filter-field");
        filterField.setPrefWidth(150);

        partitionSelector = new ComboBox<>();
        partitionSelector.getStyleClass().add("partition-selector");
        partitionSelector.setOnAction(e -> onPartitionChanged());

        Button seekBeginBtn = new Button("\u25C0\u25C0");
        seekBeginBtn.getStyleClass().add("seek-button");
        seekBeginBtn.setOnAction(e -> seekToBeginning());

        seekField = new TextField();
        seekField.setPromptText("seek to offset");
        seekField.getStyleClass().add("seek-field");
        seekField.setPrefWidth(120);
        seekField.setOnAction(e -> seekToOffset());

        Button seekEndBtn = new Button("\u25B6\u25B6");
        seekEndBtn.getStyleClass().add("seek-button");
        seekEndBtn.setOnAction(e -> seekToEnd());

        controlsRow.getChildren().addAll(filterField, partitionSelector, seekBeginBtn, seekField, seekEndBtn);

        // Showing info
        showingLabel = new Label("");
        showingLabel.getStyleClass().add("showing-label");
        showingLabel.setAlignment(Pos.CENTER_RIGHT);
        showingLabel.setMaxWidth(Double.MAX_VALUE);
        HBox showingRow = new HBox(showingLabel);
        showingRow.setAlignment(Pos.CENTER_RIGHT);

        // Table
        table = new TableView<>();
        table.getStyleClass().add("record-table");
        VBox.setVgrow(table, Priority.ALWAYS);

        TableColumn<KeyValue<String, String>, String> offsetCol = new TableColumn<>("Offset");
        offsetCol.setCellValueFactory(cell -> {
            KeyValue<String, String> kv = cell.getValue();
            String offset = kv.getMetadata().map(m -> String.valueOf(m.getOffset())).orElse("");
            return new SimpleStringProperty(offset);
        });
        offsetCol.setPrefWidth(80);

        TableColumn<KeyValue<String, String>, String> keyCol = new TableColumn<>("Key");
        keyCol.setCellValueFactory(cell -> new SimpleStringProperty(
                cell.getValue().getKey() != null ? cell.getValue().getKey() : ""));
        keyCol.setPrefWidth(200);

        TableColumn<KeyValue<String, String>, String> valueCol = new TableColumn<>("Value");
        valueCol.setCellValueFactory(cell -> new SimpleStringProperty(
                cell.getValue().getValue() != null ? cell.getValue().getValue() : ""));
        valueCol.setPrefWidth(400);

        table.getColumns().addAll(offsetCol, keyCol, valueCol);

        // Pagination buttons
        HBox paginationRow = new HBox(10);
        paginationRow.setAlignment(Pos.CENTER);

        Button firstBtn = new Button("\u25C0\u25C0");
        firstBtn.getStyleClass().add("pagination-button");
        firstBtn.setOnAction(e -> goToFirst());

        Button prevBtn = new Button("\u25C0");
        prevBtn.getStyleClass().add("pagination-button");
        prevBtn.setOnAction(e -> goToPrev());

        Button nextBtn = new Button("\u25B6");
        nextBtn.getStyleClass().add("pagination-button");
        nextBtn.setOnAction(e -> goToNext());

        Button lastBtn = new Button("\u25B6\u25B6");
        lastBtn.getStyleClass().add("pagination-button");
        lastBtn.setOnAction(e -> goToLast());

        Button produceBtn = new Button("+");
        produceBtn.getStyleClass().add("produce-button");
        produceBtn.setOnAction(e -> showProduceDialog());

        HBox spacer = new HBox();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        paginationRow.getChildren().addAll(firstBtn, prevBtn, nextBtn, lastBtn, spacer, produceBtn);

        getChildren().addAll(headerRow, controlsRow, showingRow, table, paginationRow);
    }

    public void loadTopic(String topicName) {
        this.currentTopic = topicName;
        this.currentPartition = 0;
        topicHeaderLabel.setText(topicName);

        Thread thread = new Thread(() -> {
            Map<Integer, Long> endOffsets = service.getEndOffsets(topicName);
            Map<Integer, Long> beginOffsets = service.getBeginningOffsets(topicName);
            Platform.runLater(() -> {
                partitionSelector.getItems().clear();
                for (int i = 0; i < endOffsets.size(); i++) {
                    partitionSelector.getItems().add("Partition " + i);
                }
                if (!partitionSelector.getItems().isEmpty()) {
                    partitionSelector.getSelectionModel().selectFirst();
                }
                this.endOffset = endOffsets.getOrDefault(0, 0L);
                this.beginOffset = beginOffsets.getOrDefault(0, 0L);
                // Start from the end, showing most recent records
                this.currentOffset = Math.max(0, this.endOffset - PAGE_SIZE);
                loadRecords();
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void loadRecords() {
        if (currentTopic == null) return;

        Thread thread = new Thread(() -> {
            List<KeyValue<String, String>> records = service.readRecords(
                    currentTopic, currentPartition, currentOffset, PAGE_SIZE);
            Platform.runLater(() -> {
                table.getItems().setAll(records);
                // Sort by offset descending
                Collections.reverse(table.getItems());
                updateShowingLabel();
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void updateShowingLabel() {
        if (table.getItems().isEmpty()) {
            showingLabel.setText("No records.");
        } else {
            long first = table.getItems().get(0).getMetadata()
                    .map(m -> m.getOffset()).orElse(0L);
            long last = table.getItems().get(table.getItems().size() - 1).getMetadata()
                    .map(m -> m.getOffset()).orElse(0L);
            showingLabel.setText("Showing records " + first + "-" + last + ".");
        }
    }

    private void onPartitionChanged() {
        String selected = partitionSelector.getValue();
        if (selected != null) {
            currentPartition = Integer.parseInt(selected.replace("Partition ", ""));
            Map<Integer, Long> endOffsets = service.getEndOffsets(currentTopic);
            Map<Integer, Long> beginOffsets = service.getBeginningOffsets(currentTopic);
            endOffset = endOffsets.getOrDefault(currentPartition, 0L);
            beginOffset = beginOffsets.getOrDefault(currentPartition, 0L);
            currentOffset = Math.max(0, endOffset - PAGE_SIZE);
            loadRecords();
        }
    }

    private void seekToOffset() {
        try {
            long offset = Long.parseLong(seekField.getText().trim());
            currentOffset = Math.max(beginOffset, Math.min(offset, endOffset));
            loadRecords();
        } catch (NumberFormatException ignored) {
            // Invalid input, do nothing
        }
    }

    private void seekToBeginning() {
        currentOffset = beginOffset;
        loadRecords();
    }

    private void seekToEnd() {
        currentOffset = Math.max(0, endOffset - PAGE_SIZE);
        loadRecords();
    }

    private void goToFirst() {
        currentOffset = beginOffset;
        loadRecords();
    }

    private void goToPrev() {
        currentOffset = Math.max(beginOffset, currentOffset - PAGE_SIZE);
        loadRecords();
    }

    private void goToNext() {
        currentOffset = Math.min(endOffset, currentOffset + PAGE_SIZE);
        loadRecords();
    }

    private void goToLast() {
        currentOffset = Math.max(0, endOffset - PAGE_SIZE);
        loadRecords();
    }

    private void showProduceDialog() {
        ProduceRecordDialog dialog = new ProduceRecordDialog(currentTopic);
        dialog.showAndWait().ifPresent(result -> {
            service.produceRecord(currentTopic, result.key(), result.value());
            loadRecords();
        });
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
    }
}
