package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

import java.util.ArrayList;
import java.util.List;

/**
 * In-app overlay dialog for managing all environments of the active workspace.
 * Shows a list of environments with Edit and Delete buttons.
 */
public class ManageEnvironmentsDialog extends OverlayDialog<Void> {

    private final BrowserConfig config;
    private final VBox environmentListContainer;
    private final Runnable onChanged;

    public ManageEnvironmentsDialog(BrowserConfig config, Runnable onChanged) {
        super();
        this.config = config;
        this.onChanged = onChanged;

        dialogPane.setMaxWidth(600);

        // Title
        Label title = new Label("Manage Environments");
        title.getStyleClass().add("overlay-dialog-title");

        // Environment list
        environmentListContainer = new VBox(5);
        rebuildList();

        // Close button
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button closeBtn = new Button("Close");
        closeBtn.getStyleClass().add("dialog-cancel-button");
        closeBtn.setOnAction(e -> close(null));

        buttonRow.getChildren().add(closeBtn);

        dialogPane.setSpacing(12);
        dialogPane.getChildren().addAll(title, environmentListContainer, buttonRow);
    }

    private void rebuildList() {
        environmentListContainer.getChildren().clear();
        Workspace ws = config.getActiveWorkspace();

        if (ws == null || ws.getEnvironments().isEmpty()) {
            Label empty = new Label("No environments configured.");
            empty.getStyleClass().add("dialog-description");
            environmentListContainer.getChildren().add(empty);
            return;
        }

        List<Environment> environments = new ArrayList<>(ws.getEnvironments());
        String activeEnvName = config.getActiveEnvironmentName();

        for (int i = 0; i < environments.size(); i++) {
            Environment env = environments.get(i);
            HBox row = new HBox(10);
            row.setAlignment(Pos.CENTER_LEFT);
            row.setPadding(new Insets(8, 10, 8, 10));
            row.getStyleClass().add("manage-workspace-row");

            VBox info = new VBox(2);
            HBox.setHgrow(info, Priority.ALWAYS);

            Label nameLabel = new Label(env.getName());
            nameLabel.getStyleClass().add("manage-workspace-name");
            if (env.getName().equals(activeEnvName)) {
                nameLabel.setText(env.getName() + " (active)");
            }

            Label detailLabel = new Label(env.getBootstrapServers().isEmpty() ? "Not configured" : env.getBootstrapServers());
            detailLabel.getStyleClass().add("manage-workspace-detail");

            info.getChildren().addAll(nameLabel, detailLabel);

            Button editBtn = new Button("Edit");
            editBtn.getStyleClass().add("dialog-save-button");
            editBtn.setOnAction(e -> editEnvironment(env));

            Button deleteBtn = new Button("Delete");
            deleteBtn.getStyleClass().add("dialog-delete-button");
            deleteBtn.setOnAction(e -> deleteEnvironment(env));

            row.getChildren().addAll(info, editBtn, deleteBtn);
            environmentListContainer.getChildren().add(row);

            if (i < environments.size() - 1) {
                environmentListContainer.getChildren().add(new Separator());
            }
        }
    }

    private void editEnvironment(Environment env) {
        if (getParent() instanceof javafx.scene.layout.StackPane container) {
            CustomizeEnvironmentDialog editDialog = new CustomizeEnvironmentDialog(env);
            editDialog.setOnResult(updated -> {
                if (updated != null) {
                    Workspace ws = config.getActiveWorkspace();
                    if (ws != null) {
                        List<Environment> envs = new ArrayList<>(ws.getEnvironments());
                        for (int i = 0; i < envs.size(); i++) {
                            if (envs.get(i).getName().equals(env.getName())) {
                                envs.set(i, updated);
                                break;
                            }
                        }
                        ws.setEnvironments(envs);
                        if (env.getName().equals(config.getActiveEnvironmentName())) {
                            config.setActiveEnvironmentName(updated.getName());
                        }
                        rebuildList();
                        onChanged.run();
                    }
                }
            });
            editDialog.showIn(container);
        }
    }

    private void deleteEnvironment(Environment env) {
        Workspace ws = config.getActiveWorkspace();
        if (ws != null) {
            List<Environment> envs = new ArrayList<>(ws.getEnvironments());
            envs.remove(env);
            ws.setEnvironments(envs);
            if (env.getName().equals(config.getActiveEnvironmentName())) {
                // Switch to first available or null
                config.setActiveEnvironmentName(envs.isEmpty() ? null : envs.get(0).getName());
            }
            rebuildList();
            onChanged.run();
        }
    }
}
