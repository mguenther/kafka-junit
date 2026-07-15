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
    private ComboBox<String> keyDeserializerCombo;
    private ComboBox<String> valueDeserializerCombo;
    private TextField seekField;
    private Button filterToggleBtn;
    private Label filterCountLabel;
    private VBox filterPanel;
    private VBox activeFiltersContainer;
    private TableView<KeyValue<String, String>> table;
    private List<KeyValue<String, String>> unfilteredRecords = Collections.emptyList();
    private final List<RecordFilter> activeFilters = new java.util.ArrayList<>();

    private record RecordFilter(String type, String value) {}

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

        // Controls row: filter toggle, partition, seek
        HBox controlsRow = new HBox(8);
        controlsRow.setAlignment(Pos.CENTER_LEFT);
        controlsRow.getStyleClass().add("record-controls-bar");

        filterToggleBtn = new Button("\uD83D\uDD0D");
        filterToggleBtn.getStyleClass().add("filter-toggle-button");
        filterToggleBtn.setOnAction(e -> toggleFilterPanel());

        filterCountLabel = new Label("");
        filterCountLabel.getStyleClass().add("filter-count-label");
        filterCountLabel.setVisible(false);

        Label keyDesLabel = new Label("Key:");
        keyDesLabel.getStyleClass().add("deserializer-label");

        keyDeserializerCombo = new ComboBox<>();
        keyDeserializerCombo.getItems().addAll("String", "Double", "Float", "Integer", "Long", "Short", "UUID");
        keyDeserializerCombo.getSelectionModel().selectFirst();
        keyDeserializerCombo.getStyleClass().add("controls-combo");
        keyDeserializerCombo.setOnAction(e -> loadRecords());

        Label valueDesLabel = new Label("Value:");
        valueDesLabel.getStyleClass().add("deserializer-label");

        valueDeserializerCombo = new ComboBox<>();
        valueDeserializerCombo.getItems().addAll("String", "Double", "Float", "Integer", "Long", "Short", "UUID");
        valueDeserializerCombo.getSelectionModel().selectFirst();
        valueDeserializerCombo.getStyleClass().add("controls-combo");
        valueDeserializerCombo.setOnAction(e -> loadRecords());

        partitionSelector = new ComboBox<>();
        partitionSelector.getStyleClass().add("controls-combo");
        partitionSelector.setOnAction(e -> onPartitionChanged());

        Button seekBeginBtn = new Button("\u23EE");
        seekBeginBtn.getStyleClass().add("seek-button");
        seekBeginBtn.setOnAction(e -> seekToBeginning());

        seekField = new TextField();
        seekField.setPromptText("seek to offset");
        seekField.getStyleClass().add("seek-field");
        seekField.setPrefWidth(120);
        seekField.setOnAction(e -> seekToOffset());

        Button seekEndBtn = new Button("\u23ED");
        seekEndBtn.getStyleClass().add("seek-button");
        seekEndBtn.setOnAction(e -> seekToEnd());

        Label partitionLabel = new Label("Partition:");
        partitionLabel.getStyleClass().add("deserializer-label");

        controlsRow.getChildren().addAll(
                filterToggleBtn, filterCountLabel,
                keyDesLabel, keyDeserializerCombo,
                valueDesLabel, valueDeserializerCombo,
                partitionLabel, partitionSelector,
                seekBeginBtn, seekField, seekEndBtn
        );

        // Expandable filter panel (hidden by default)
        filterPanel = new VBox(8);
        filterPanel.getStyleClass().add("filter-panel");
        filterPanel.setVisible(false);
        filterPanel.setManaged(false);

        // Active filters list
        activeFiltersContainer = new VBox(4);
        activeFiltersContainer.getStyleClass().add("active-filters-container");

        // Add filter row (always visible at the bottom of the panel)
        HBox addFilterRow = buildAddFilterRow();

        filterPanel.getChildren().addAll(activeFiltersContainer, addFilterRow);

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
        table.setPlaceholder(new Label("No records in table or current selection."));
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
                singleLine(cell.getValue().getKey())));
        keyCol.setPrefWidth(200);

        TableColumn<KeyValue<String, String>, String> valueCol = new TableColumn<>("Value");
        valueCol.setCellValueFactory(cell -> new SimpleStringProperty(
                singleLine(cell.getValue().getValue())));
        valueCol.setPrefWidth(400);

        table.getColumns().addAll(offsetCol, keyCol, valueCol);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        // Single-line row height
        table.setFixedCellSize(32);

        // Click on a row to open full record detail
        table.setOnMouseClicked(e -> {
            if (e.getClickCount() == 2) {
                KeyValue<String, String> selected = table.getSelectionModel().getSelectedItem();
                if (selected != null) {
                    showRecordDetail(selected);
                }
            }
        });

        // Pagination buttons
        HBox paginationRow = new HBox(10);
        paginationRow.setAlignment(Pos.CENTER);

        Button firstBtn = new Button("\u23EE");
        firstBtn.getStyleClass().add("pagination-button");
        firstBtn.setOnAction(e -> goToFirst());

        Button prevBtn = new Button("\u25C0");
        prevBtn.getStyleClass().add("pagination-button");
        prevBtn.setOnAction(e -> goToPrev());

        Button nextBtn = new Button("\u25B6");
        nextBtn.getStyleClass().add("pagination-button");
        nextBtn.setOnAction(e -> goToNext());

        Button lastBtn = new Button("\u23ED");
        lastBtn.getStyleClass().add("pagination-button");
        lastBtn.setOnAction(e -> goToLast());

        Button produceBtn = new Button("+");
        produceBtn.getStyleClass().add("produce-button");
        produceBtn.setOnAction(e -> showProduceDialog());

        HBox spacer = new HBox();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        paginationRow.getChildren().addAll(firstBtn, prevBtn, nextBtn, lastBtn, spacer, produceBtn);

        getChildren().addAll(headerRow, controlsRow, filterPanel, showingRow, table, paginationRow);
    }

    public void loadTopic(String topicName) {
        this.currentTopic = topicName;
        this.currentPartition = -1; // -1 means all partitions
        topicHeaderLabel.setText(topicName);

        // Clear table immediately and show loading state
        table.getItems().clear();
        table.setPlaceholder(new Label("Loading records..."));
        showingLabel.setText("");

        Thread thread = new Thread(() -> {
            Map<Integer, Long> endOffsets = service.getEndOffsets(topicName);
            Map<Integer, Long> beginOffsets = service.getBeginningOffsets(topicName);
            Platform.runLater(() -> {
                partitionSelector.getItems().clear();
                partitionSelector.getItems().add("All");
                for (int i = 0; i < endOffsets.size(); i++) {
                    partitionSelector.getItems().add(String.valueOf(i));
                }
                partitionSelector.getSelectionModel().selectFirst(); // "All"

                // Compute total end/begin across all partitions
                this.endOffset = endOffsets.values().stream().mapToLong(Long::longValue).max().orElse(0L);
                this.beginOffset = beginOffsets.values().stream().mapToLong(Long::longValue).min().orElse(0L);
                this.currentOffset = 0;
                loadRecords();
            });
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void loadRecords() {
        if (currentTopic == null) return;

        // Clear table and show loading state
        table.getItems().clear();
        table.setPlaceholder(new Label("Loading records..."));
        showingLabel.setText("");

        String keyDes = keyDeserializerCombo.getValue();
        String valueDes = valueDeserializerCombo.getValue();

        Thread thread = new Thread(() -> {
            try {
                List<KeyValue<String, String>> records;
                if (currentPartition < 0) {
                    records = service.readRecordsAllPartitions(currentTopic, PAGE_SIZE, keyDes, valueDes);
                } else {
                    records = service.readRecords(
                            currentTopic, currentPartition, currentOffset, PAGE_SIZE, keyDes, valueDes);
                }
                Platform.runLater(() -> {
                    table.setPlaceholder(new Label("No records in table or current selection."));
                    unfilteredRecords = records;
                    applyFilters();
                });
            } catch (net.mguenther.kafka.browser.service.DeserializationException e) {
                Platform.runLater(() -> {
                    Label errorLabel = new Label("\u26A0 Unable to deserialize records due to mismatching key/value deserializers.");
                    errorLabel.getStyleClass().add("table-error-placeholder");
                    errorLabel.setWrapText(true);
                    table.setPlaceholder(errorLabel);
                    unfilteredRecords = Collections.emptyList();
                });
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private String singleLine(String s) {
        if (s == null || s.isEmpty()) return "";
        return s.replaceAll("\\s+", " ").trim();
    }

    private String getDeserializerClassName(String shortName) {
        if (shortName == null) return null;
        return switch (shortName) {
            case "String" -> "org.apache.kafka.common.serialization.StringDeserializer";
            case "Double" -> "org.apache.kafka.common.serialization.DoubleDeserializer";
            case "Float" -> "org.apache.kafka.common.serialization.FloatDeserializer";
            case "Integer" -> "org.apache.kafka.common.serialization.IntegerDeserializer";
            case "Long" -> "org.apache.kafka.common.serialization.LongDeserializer";
            case "Short" -> "org.apache.kafka.common.serialization.ShortDeserializer";
            case "UUID" -> "org.apache.kafka.common.serialization.UUIDDeserializer";
            default -> null;
        };
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

    private void toggleFilterPanel() {
        boolean show = !filterPanel.isVisible();
        filterPanel.setVisible(show);
        filterPanel.setManaged(show);
        filterToggleBtn.getStyleClass().remove("filter-toggle-button-active");
        if (show) {
            filterToggleBtn.getStyleClass().add("filter-toggle-button-active");
            filterCountLabel.setVisible(false);
        } else {
            updateFilterCountLabel();
        }
    }

    private HBox buildAddFilterRow() {
        HBox row = new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);
        row.getStyleClass().add("add-filter-row");

        ComboBox<String> typeSelector = new ComboBox<>();
        typeSelector.getItems().addAll("Key", "Value", "Header");
        typeSelector.getSelectionModel().selectFirst();
        typeSelector.getStyleClass().add("filter-type-selector");

        TextField filterInput = new TextField();
        filterInput.setPromptText("contains...");
        filterInput.getStyleClass().add("filter-bar-field");
        filterInput.setPrefWidth(200);
        HBox.setHgrow(filterInput, Priority.ALWAYS);

        Button addBtn = new Button("Add");
        addBtn.getStyleClass().add("dialog-save-button");
        addBtn.setOnAction(e -> {
            String value = filterInput.getText().trim();
            if (!value.isEmpty()) {
                addFilter(typeSelector.getValue(), value);
                filterInput.clear();
            }
        });

        filterInput.setOnAction(e -> addBtn.fire());

        row.getChildren().addAll(typeSelector, filterInput, addBtn);
        return row;
    }

    private void addFilter(String type, String value) {
        RecordFilter filter = new RecordFilter(type, value);
        activeFilters.add(filter);
        rebuildFilterList();
        loadRecords();
    }

    private void removeFilter(RecordFilter filter) {
        activeFilters.remove(filter);
        rebuildFilterList();
        loadRecords();
    }

    private void rebuildFilterList() {
        activeFiltersContainer.getChildren().clear();
        for (RecordFilter filter : activeFilters) {
            HBox chip = new HBox(6);
            chip.setAlignment(Pos.CENTER_LEFT);
            chip.getStyleClass().add("filter-chip");

            Label typeLabel = new Label(filter.type());
            typeLabel.getStyleClass().add("filter-chip-type");

            Label valueLabel = new Label("contains \"" + filter.value() + "\"");
            valueLabel.getStyleClass().add("filter-chip-value");
            HBox.setHgrow(valueLabel, Priority.ALWAYS);

            Button removeBtn = new Button("\u2715");
            removeBtn.getStyleClass().add("filter-chip-remove");
            removeBtn.setOnAction(e -> removeFilter(filter));

            chip.getChildren().addAll(typeLabel, valueLabel, removeBtn);
            activeFiltersContainer.getChildren().add(chip);
        }

        if (!activeFilters.isEmpty()) {
            HBox clearRow = new HBox();
            clearRow.setAlignment(Pos.CENTER_RIGHT);
            Button clearAllBtn = new Button("Clear all");
            clearAllBtn.getStyleClass().add("filter-clear-button");
            clearAllBtn.setOnAction(e -> {
                activeFilters.clear();
                rebuildFilterList();
                loadRecords();
            });
            clearRow.getChildren().add(clearAllBtn);
            activeFiltersContainer.getChildren().add(clearRow);
        }

        updateFilterCountLabel();
    }

    private void updateFilterCountLabel() {
        if (activeFilters.isEmpty()) {
            filterCountLabel.setVisible(false);
        } else if (!filterPanel.isVisible()) {
            filterCountLabel.setText(activeFilters.size() + " active " + (activeFilters.size() == 1 ? "filter" : "filters"));
            filterCountLabel.setVisible(true);
        } else {
            filterCountLabel.setVisible(false);
        }
    }

    private void applyFilters() {
        List<KeyValue<String, String>> filtered = unfilteredRecords.stream()
                .filter(kv -> {
                    for (RecordFilter filter : activeFilters) {
                        String term = filter.value().toLowerCase();
                        switch (filter.type()) {
                            case "Key" -> {
                                String key = kv.getKey() != null ? kv.getKey().toLowerCase() : "";
                                if (!key.contains(term)) return false;
                            }
                            case "Value" -> {
                                String value = kv.getValue() != null ? kv.getValue().toLowerCase() : "";
                                if (!value.contains(term)) return false;
                            }
                            case "Header" -> {
                                boolean headerMatch = false;
                                if (kv.getHeaders() != null) {
                                    for (org.apache.kafka.common.header.Header h : kv.getHeaders()) {
                                        String hKey = h.key() != null ? h.key().toLowerCase() : "";
                                        String hVal = h.value() != null
                                                ? new String(h.value(), java.nio.charset.StandardCharsets.UTF_8).toLowerCase()
                                                : "";
                                        if (hKey.contains(term) || hVal.contains(term)) {
                                            headerMatch = true;
                                            break;
                                        }
                                    }
                                }
                                if (!headerMatch) return false;
                            }
                        }
                    }
                    return true;
                })
                .toList();

        table.getItems().setAll(filtered);
        Collections.reverse(table.getItems());
        updateShowingLabel();
    }

    private void onPartitionChanged() {
        String selected = partitionSelector.getValue();
        if (selected == null) return;

        if ("All".equals(selected)) {
            currentPartition = -1;
            currentOffset = 0;
            loadRecords();
        } else {
            currentPartition = Integer.parseInt(selected);
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
        ProduceRecordDialog dialog = new ProduceRecordDialog(currentTopic,
                keyDeserializerCombo.getValue(), valueDeserializerCombo.getValue());
        dialog.setOnResult(result -> {
            if (result != null) {
                service.produceRecord(
                        currentTopic,
                        result.key(),
                        result.value(),
                        result.keySerializer(),
                        result.valueSerializer(),
                        result.headers()
                );
                loadRecords();
            }
        });
        // Find the nearest StackPane ancestor to use as overlay container
        javafx.scene.Node node = this;
        while (node != null && !(node instanceof javafx.scene.layout.StackPane)) {
            node = node.getParent();
        }
        if (node instanceof javafx.scene.layout.StackPane sp) {
            dialog.showIn(sp);
        }
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
    }

    private void showRecordDetail(KeyValue<String, String> record) {
        RecordDetailDialog dialog = new RecordDetailDialog(record, currentTopic);
        javafx.scene.Node node = this;
        while (node != null && !(node instanceof javafx.scene.layout.StackPane)) {
            node = node.getParent();
        }
        if (node instanceof javafx.scene.layout.StackPane sp) {
            dialog.showIn(sp);
        }
    }
}
