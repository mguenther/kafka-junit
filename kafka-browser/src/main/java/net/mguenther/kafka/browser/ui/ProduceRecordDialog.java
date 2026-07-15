package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-app overlay dialog to produce a new key-value record to a topic.
 * Supports:
 * - Key and Value serializer selection (all built-in Kafka serializers)
 * - Key and Value input
 * - Optional record headers (visually de-emphasized to indicate optionality)
 */
public class ProduceRecordDialog extends OverlayDialog<ProduceRecordDialog.RecordData> {

    /**
     * Holds the data collected from the dialog.
     */
    public record RecordData(
            String key,
            String value,
            String keySerializer,
            String valueSerializer,
            Map<String, String> headers
    ) {}

    private static final List<SerializerOption> SERIALIZERS = List.of(
            new SerializerOption("String", "org.apache.kafka.common.serialization.StringSerializer"),
            new SerializerOption("Double", "org.apache.kafka.common.serialization.DoubleSerializer"),
            new SerializerOption("Float", "org.apache.kafka.common.serialization.FloatSerializer"),
            new SerializerOption("Integer", "org.apache.kafka.common.serialization.IntegerSerializer"),
            new SerializerOption("Long", "org.apache.kafka.common.serialization.LongSerializer"),
            new SerializerOption("Short", "org.apache.kafka.common.serialization.ShortSerializer"),
            new SerializerOption("UUID", "org.apache.kafka.common.serialization.UUIDSerializer"),
            new SerializerOption("Void", "org.apache.kafka.common.serialization.VoidSerializer")
    );

    private record SerializerOption(String displayName, String className) {
        @Override
        public String toString() {
            return displayName;
        }
    }

    private ComboBox<SerializerOption> keySerializerCombo;
    private ComboBox<SerializerOption> valueSerializerCombo;
    private TextField keyField;
    private TextArea valueArea;
    private VBox headersContainer;
    private final List<HeaderRow> headerRows = new ArrayList<>();
    private Label mismatchWarning;
    private String browserKeyDeserializer;
    private String browserValueDeserializer;

    public ProduceRecordDialog(String topicName) {
        this(topicName, null, null);
    }

    public ProduceRecordDialog(String topicName, String browserKeyDeserializer, String browserValueDeserializer) {
        super();
        this.browserKeyDeserializer = browserKeyDeserializer;
        this.browserValueDeserializer = browserValueDeserializer;

        dialogPane.setMaxWidth(600);

        // Title
        Label title = new Label("Produce Record to " + topicName);
        title.getStyleClass().add("overlay-dialog-title");

        // --- Key section ---
        HBox keyHeaderRow = new HBox(15);
        keyHeaderRow.setAlignment(Pos.CENTER_LEFT);

        Label keyLabel = new Label("Key");
        keyLabel.getStyleClass().add("dialog-field-label");
        HBox.setHgrow(keyLabel, Priority.ALWAYS);

        Label keySerLabel = new Label("Serializer:");
        keySerLabel.getStyleClass().add("dialog-field-hint");

        keySerializerCombo = new ComboBox<>();
        keySerializerCombo.getItems().addAll(SERIALIZERS);
        keySerializerCombo.getSelectionModel().selectFirst(); // Default: String
        selectSerializerByName(keySerializerCombo, browserKeyDeserializer);
        keySerializerCombo.getStyleClass().add("dialog-combo-box");

        Button keyGenUuidBtn = new Button("Generate");
        keyGenUuidBtn.getStyleClass().add("dialog-save-button");
        keyGenUuidBtn.setVisible(false);
        keyGenUuidBtn.setManaged(false);
        keyGenUuidBtn.setOnAction(e -> keyField.setText(java.util.UUID.randomUUID().toString()));

        keySerializerCombo.valueProperty().addListener((obs, oldVal, newVal) -> {
            boolean isUuid = newVal != null && "UUID".equals(newVal.displayName());
            keyGenUuidBtn.setVisible(isUuid);
            keyGenUuidBtn.setManaged(isUuid);
        });

        keyHeaderRow.getChildren().addAll(keyLabel, keySerLabel, keySerializerCombo, keyGenUuidBtn);

        keyField = new TextField();
        keyField.setPromptText("Record key");
        keyField.getStyleClass().add("dialog-text-field");

        // --- Value section ---
        HBox valueHeaderRow = new HBox(15);
        valueHeaderRow.setAlignment(Pos.CENTER_LEFT);

        Label valueLabel = new Label("Value");
        valueLabel.getStyleClass().add("dialog-field-label");
        HBox.setHgrow(valueLabel, Priority.ALWAYS);

        Label valueSerLabel = new Label("Serializer:");
        valueSerLabel.getStyleClass().add("dialog-field-hint");

        valueSerializerCombo = new ComboBox<>();
        valueSerializerCombo.getItems().addAll(SERIALIZERS);
        valueSerializerCombo.getSelectionModel().selectFirst(); // Default: String
        selectSerializerByName(valueSerializerCombo, browserValueDeserializer);
        valueSerializerCombo.getStyleClass().add("dialog-combo-box");

        Button valueGenUuidBtn = new Button("Generate");
        valueGenUuidBtn.getStyleClass().add("dialog-save-button");
        valueGenUuidBtn.setVisible(false);
        valueGenUuidBtn.setManaged(false);
        valueGenUuidBtn.setOnAction(e -> valueArea.setText(java.util.UUID.randomUUID().toString()));

        valueSerializerCombo.valueProperty().addListener((obs, oldVal, newVal) -> {
            boolean isUuid = newVal != null && "UUID".equals(newVal.displayName());
            valueGenUuidBtn.setVisible(isUuid);
            valueGenUuidBtn.setManaged(isUuid);
        });

        valueHeaderRow.getChildren().addAll(valueLabel, valueSerLabel, valueSerializerCombo, valueGenUuidBtn);

        valueArea = new TextArea();
        valueArea.setPromptText("Record value (JSON, text, ...)");
        valueArea.getStyleClass().add("dialog-text-area");
        valueArea.setPrefRowCount(5);

        // Mismatch warning
        mismatchWarning = new Label();
        mismatchWarning.getStyleClass().add("produce-mismatch-warning");
        mismatchWarning.setWrapText(true);
        mismatchWarning.setVisible(false);
        mismatchWarning.setManaged(false);

        // Listen for serializer changes to check mismatch
        keySerializerCombo.valueProperty().addListener((obs, o, n) -> checkMismatch());
        valueSerializerCombo.valueProperty().addListener((obs, o, n) -> checkMismatch());

        // --- Headers section (optional, visually de-emphasized) ---
        Separator headerSep = new Separator();
        VBox.setMargin(headerSep, new Insets(8, 0, 4, 0));

        HBox headersLabelRow = new HBox(8);
        headersLabelRow.setAlignment(Pos.CENTER_LEFT);

        Label headersLabel = new Label("Headers");
        headersLabel.getStyleClass().add("dialog-field-label-optional");

        Label optionalBadge = new Label("optional");
        optionalBadge.getStyleClass().add("optional-badge");

        headersLabelRow.getChildren().addAll(headersLabel, optionalBadge);

        // Column hints
        HBox headersColumnHints = new HBox(10);
        headersColumnHints.setPadding(new Insets(0, 0, 0, 0));
        Label hdrKeyHint = new Label("Name");
        hdrKeyHint.getStyleClass().add("dialog-field-hint");
        hdrKeyHint.setPrefWidth(200);
        Label hdrValHint = new Label("Value");
        hdrValHint.getStyleClass().add("dialog-field-hint");
        hdrValHint.setPrefWidth(250);
        headersColumnHints.getChildren().addAll(hdrKeyHint, hdrValHint);

        headersContainer = new VBox(5);
        headersContainer.getStyleClass().add("headers-container");
        addEmptyHeaderRow();

        // --- Buttons ---
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button sendBtn = new Button("Send");
        sendBtn.getStyleClass().add("dialog-save-button");
        sendBtn.setOnAction(e -> {
            Map<String, String> headers = new LinkedHashMap<>();
            for (HeaderRow row : headerRows) {
                String name = row.nameField.getText().trim();
                String value = row.valueField.getText().trim();
                if (!name.isEmpty()) {
                    headers.put(name, value);
                }
            }
            close(new RecordData(
                    keyField.getText(),
                    valueArea.getText(),
                    keySerializerCombo.getValue().className(),
                    valueSerializerCombo.getValue().className(),
                    headers
            ));
        });

        buttonRow.getChildren().addAll(cancelBtn, sendBtn);

        dialogPane.setSpacing(8);
        dialogPane.getChildren().addAll(
                title,
                keyHeaderRow, keyField,
                valueHeaderRow, valueArea,
                mismatchWarning,
                headerSep,
                headersLabelRow, headersColumnHints, headersContainer,
                buttonRow
        );
    }

    private void selectSerializerByName(ComboBox<SerializerOption> combo, String name) {
        if (name == null || name.isEmpty()) return;
        for (SerializerOption option : combo.getItems()) {
            if (option.displayName().equals(name)) {
                combo.getSelectionModel().select(option);
                return;
            }
        }
    }

    private void checkMismatch() {
        if (browserKeyDeserializer == null && browserValueDeserializer == null) {
            mismatchWarning.setVisible(false);
            mismatchWarning.setManaged(false);
            return;
        }

        StringBuilder warning = new StringBuilder();
        SerializerOption keySer = keySerializerCombo.getValue();
        SerializerOption valueSer = valueSerializerCombo.getValue();

        if (keySer != null && browserKeyDeserializer != null
                && !keySer.displayName().equals(browserKeyDeserializer)) {
            warning.append("Key serializer (").append(keySer.displayName())
                    .append(") differs from browser deserializer (").append(browserKeyDeserializer).append("). ");
        }
        if (valueSer != null && browserValueDeserializer != null
                && !valueSer.displayName().equals(browserValueDeserializer)) {
            warning.append("Value serializer (").append(valueSer.displayName())
                    .append(") differs from browser deserializer (").append(browserValueDeserializer).append("). ");
        }

        if (warning.length() > 0) {
            warning.append("The produced record may not be readable with the current browser settings.");
            mismatchWarning.setText("\u26A0 " + warning.toString());
            mismatchWarning.setVisible(true);
            mismatchWarning.setManaged(true);
        } else {
            mismatchWarning.setVisible(false);
            mismatchWarning.setManaged(false);
        }
    }

    private void addEmptyHeaderRow() {
        HeaderRow row = new HeaderRow("", "");
        headersContainer.getChildren().add(row.container);

        row.nameField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.isEmpty() && !headerRows.contains(row)) {
                headerRows.add(row);
                row.removeBtn.setVisible(true);
                row.removeBtn.setManaged(true);
                addEmptyHeaderRow();
            }
        });
    }

    private void removeHeaderRow(HeaderRow row) {
        headerRows.remove(row);
        headersContainer.getChildren().remove(row.container);
    }

    private class HeaderRow {
        HBox container;
        TextField nameField;
        TextField valueField;
        Button removeBtn;

        HeaderRow(String name, String value) {
            container = new HBox(10);
            container.setAlignment(Pos.CENTER_LEFT);
            container.getStyleClass().add("header-row-optional");

            nameField = new TextField(name);
            nameField.setPromptText("header name");
            nameField.getStyleClass().add("dialog-text-field-optional");
            nameField.setPrefWidth(200);

            valueField = new TextField(value);
            valueField.setPromptText("header value");
            valueField.getStyleClass().add("dialog-text-field-optional");
            valueField.setPrefWidth(250);

            removeBtn = new Button("\u2212");
            removeBtn.getStyleClass().add("remove-param-button");
            removeBtn.setVisible(false);
            removeBtn.setManaged(false);
            removeBtn.setOnAction(e -> removeHeaderRow(this));

            container.getChildren().addAll(nameField, valueField, removeBtn);
        }
    }
}
