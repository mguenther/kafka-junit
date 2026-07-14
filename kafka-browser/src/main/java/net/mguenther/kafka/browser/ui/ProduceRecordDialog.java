package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

/**
 * Simple dialog to produce a new key-value record to the current topic.
 */
public class ProduceRecordDialog extends Dialog<ProduceRecordDialog.RecordData> {

    public record RecordData(String key, String value) {}

    public ProduceRecordDialog(String topicName) {
        setTitle("Produce Record to " + topicName);
        setHeaderText(null);

        VBox content = new VBox(15);
        content.setPadding(new Insets(20));
        content.setPrefWidth(500);

        Label keyLabel = new Label("Key");
        keyLabel.getStyleClass().add("dialog-field-label");
        TextField keyField = new TextField();
        keyField.setPromptText("Record key");
        keyField.getStyleClass().add("dialog-text-field");

        Label valueLabel = new Label("Value");
        valueLabel.getStyleClass().add("dialog-field-label");
        TextArea valueArea = new TextArea();
        valueArea.setPromptText("Record value (JSON, text, ...)");
        valueArea.getStyleClass().add("dialog-text-area");
        valueArea.setPrefRowCount(6);

        HBox buttonRow = new HBox(10);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));
        buttonRow.setStyle("-fx-alignment: center-right;");

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> {
            setResult(null);
            close();
        });

        Button sendBtn = new Button("Send");
        sendBtn.getStyleClass().add("dialog-save-button");
        sendBtn.setOnAction(e -> {
            setResult(new RecordData(keyField.getText(), valueArea.getText()));
            close();
        });

        buttonRow.getChildren().addAll(cancelBtn, sendBtn);
        content.getChildren().addAll(keyLabel, keyField, valueLabel, valueArea, buttonRow);

        getDialogPane().setContent(content);
        getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        getDialogPane().lookupButton(ButtonType.CLOSE).setVisible(false);
        getDialogPane().lookupButton(ButtonType.CLOSE).setManaged(false);

        setResultConverter(bt -> null);
    }
}
