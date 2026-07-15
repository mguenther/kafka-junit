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
import net.mguenther.kafka.browser.model.Workspace;

import java.util.ArrayList;
import java.util.List;

/**
 * In-app overlay dialog for managing all workspaces.
 * Shows a list of workspaces with Edit and Delete buttons.
 */
public class ManageWorkspacesDialog extends OverlayDialog<Void> {

    private final BrowserConfig config;
    private final VBox workspaceListContainer;
    private final Runnable onChanged;

    public ManageWorkspacesDialog(BrowserConfig config, Runnable onChanged) {
        super();
        this.config = config;
        this.onChanged = onChanged;

        dialogPane.setMaxWidth(600);

        // Title
        Label title = new Label("Manage Workspaces");
        title.getStyleClass().add("overlay-dialog-title");

        // Workspace list
        workspaceListContainer = new VBox(5);
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
        dialogPane.getChildren().addAll(title, workspaceListContainer, buttonRow);
    }

    private void rebuildList() {
        workspaceListContainer.getChildren().clear();
        List<Workspace> workspaces = new ArrayList<>(config.getWorkspaces());

        if (workspaces.isEmpty()) {
            Label empty = new Label("No workspaces configured.");
            empty.getStyleClass().add("dialog-description");
            workspaceListContainer.getChildren().add(empty);
            return;
        }

        for (Workspace ws : workspaces) {
            HBox row = new HBox(10);
            row.setAlignment(Pos.CENTER_LEFT);
            row.setPadding(new Insets(8, 10, 8, 10));
            row.getStyleClass().add("manage-workspace-row");

            VBox info = new VBox(2);
            HBox.setHgrow(info, Priority.ALWAYS);

            Label nameLabel = new Label(ws.getName());
            nameLabel.getStyleClass().add("manage-workspace-name");

            Label detailLabel = new Label(ws.getTopicFilter().isEmpty() ? "All topics" : "Filter: " + ws.getTopicFilter());
            detailLabel.getStyleClass().add("manage-workspace-detail");

            info.getChildren().addAll(nameLabel, detailLabel);

            Button editBtn = new Button("Edit");
            editBtn.getStyleClass().add("dialog-save-button");
            editBtn.setOnAction(e -> editWorkspace(ws));

            Button deleteBtn = new Button("Delete");
            deleteBtn.getStyleClass().add("dialog-delete-button");
            deleteBtn.setOnAction(e -> deleteWorkspace(ws));

            row.getChildren().addAll(info, editBtn, deleteBtn);
            workspaceListContainer.getChildren().add(row);

            if (workspaces.indexOf(ws) < workspaces.size() - 1) {
                workspaceListContainer.getChildren().add(new Separator());
            }
        }
    }

    private void editWorkspace(Workspace ws) {
        // We need to get the parent StackPane to show the nested dialog
        if (getParent() instanceof javafx.scene.layout.StackPane container) {
            CreateWorkspaceDialog editDialog = new CreateWorkspaceDialog(ws);
            editDialog.setOnResult(updated -> {
                if (updated != null) {
                    ws.setName(updated.getName());
                    ws.setTopicFilter(updated.getTopicFilter());
                    rebuildList();
                    onChanged.run();
                }
            });
            editDialog.showIn(container);
        }
    }

    private void deleteWorkspace(Workspace ws) {
        config.removeWorkspace(ws);
        rebuildList();
        onChanged.run();
    }
}
