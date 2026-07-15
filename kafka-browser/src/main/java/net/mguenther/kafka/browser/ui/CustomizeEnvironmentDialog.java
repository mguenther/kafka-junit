package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.Environment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-app overlay dialog for customizing an environment's key-value parameters.
 * Shows a dynamic table with Parameter/Value columns and remove buttons,
 * plus an empty row for adding new parameters.
 */
public class CustomizeEnvironmentDialog extends OverlayDialog<Environment> {

    private final Environment environment;
    private VBox rowsContainer;
    private final List<ParameterRow> parameterRows = new ArrayList<>();

    public CustomizeEnvironmentDialog(Environment environment) {
        super();
        this.environment = environment;

        dialogPane.setMaxWidth(700);

        // Title
        Label title = new Label("Customize Environment " + environment.getName());
        title.getStyleClass().add("overlay-dialog-title");

        // Column headers
        HBox headerRow = new HBox(10);
        headerRow.setAlignment(Pos.CENTER_LEFT);
        headerRow.setPadding(new Insets(10, 0, 0, 0));

        Label paramHeader = new Label("Parameter");
        paramHeader.getStyleClass().add("dialog-field-label");
        paramHeader.setPrefWidth(250);

        Label valueHeader = new Label("Value");
        valueHeader.getStyleClass().add("dialog-field-label");
        valueHeader.setPrefWidth(250);

        headerRow.getChildren().addAll(paramHeader, valueHeader);

        // Rows container
        rowsContainer = new VBox(8);

        // Add existing parameters
        for (Map.Entry<String, String> entry : environment.getParameters().entrySet()) {
            addParameterRow(entry.getKey(), entry.getValue());
        }

        // Add empty row for new entry
        addEmptyRow();

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button saveBtn = new Button("Save");
        saveBtn.getStyleClass().add("dialog-save-button");
        saveBtn.setOnAction(e -> {
            Map<String, String> params = new LinkedHashMap<>();
            for (ParameterRow row : parameterRows) {
                String key = row.keyField.getText().trim();
                String value = row.valueField.getText().trim();
                if (!key.isEmpty()) {
                    params.put(key, value);
                }
            }
            close(new Environment(environment.getName(), params));
        });

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);

        dialogPane.setSpacing(8);
        dialogPane.getChildren().addAll(title, headerRow, rowsContainer, buttonRow);
    }

    private void addParameterRow(String key, String value) {
        ParameterRow row = new ParameterRow(key, value, true);
        parameterRows.add(row);
        rowsContainer.getChildren().add(row.container);
    }

    private void addEmptyRow() {
        ParameterRow row = new ParameterRow("", "", false);
        rowsContainer.getChildren().add(row.container);

        row.keyField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.isEmpty() && !parameterRows.contains(row)) {
                parameterRows.add(row);
                row.removeBtn.setVisible(true);
                row.removeBtn.setManaged(true);
                addEmptyRow();
            }
        });
    }

    private void removeRow(ParameterRow row) {
        parameterRows.remove(row);
        rowsContainer.getChildren().remove(row.container);
    }

    private class ParameterRow {
        HBox container;
        TextField keyField;
        TextField valueField;
        Button removeBtn;

        ParameterRow(String key, String value, boolean showRemove) {
            container = new HBox(10);
            container.setAlignment(Pos.CENTER_LEFT);

            keyField = new TextField(key);
            keyField.setPromptText("<new parameter>");
            keyField.getStyleClass().add("dialog-text-field");
            keyField.setPrefWidth(250);

            valueField = new TextField(value);
            valueField.setPromptText("<new value>");
            valueField.getStyleClass().add("dialog-text-field");
            valueField.setPrefWidth(250);

            removeBtn = new Button("\u2212");
            removeBtn.getStyleClass().add("remove-param-button");
            removeBtn.setVisible(showRemove);
            removeBtn.setManaged(showRemove);
            removeBtn.setOnAction(e -> removeRow(this));

            container.getChildren().addAll(keyField, valueField, removeBtn);
        }
    }
}
