package net.mguenther.kafka.browser.ui;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

/**
 * Top header bar with two dropdown areas:
 * - Left half: Workspace selector (accent background)
 * - Right half: Environment selector (light background)
 */
public class HeaderBar extends HBox {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onWorkspaceChanged;
    private final Runnable onEnvironmentChanged;
    private StackPane overlayContainer;

    private Label workspaceLabel;
    private Label environmentLabel;
    private HBox workspaceBox;
    private HBox environmentBox;

    public HeaderBar(BrowserConfig config, ConfigPersistence persistence,
                     Runnable onWorkspaceChanged, Runnable onEnvironmentChanged) {
        this.config = config;
        this.persistence = persistence;
        this.onWorkspaceChanged = onWorkspaceChanged;
        this.onEnvironmentChanged = onEnvironmentChanged;

        getStyleClass().add("header-bar");
        setPrefHeight(40);
        setMinHeight(40);
        setMaxHeight(40);

        buildUI();
    }

    /**
     * Sets the overlay container used to display in-app dialogs.
     * Must be called after construction and before user interaction.
     */
    public void setOverlayContainer(StackPane overlayContainer) {
        this.overlayContainer = overlayContainer;
    }

    private void buildUI() {
        // Workspace selector (left half)
        workspaceBox = new HBox();
        workspaceBox.getStyleClass().add("header-workspace");
        workspaceBox.setAlignment(Pos.CENTER);
        HBox.setHgrow(workspaceBox, Priority.ALWAYS);

        workspaceLabel = new Label(getWorkspaceDisplayName());
        workspaceLabel.getStyleClass().add("header-workspace-label");

        Label workspaceArrow = new Label(" \u25BC");
        workspaceArrow.getStyleClass().add("header-workspace-label");

        workspaceBox.getChildren().addAll(workspaceLabel, workspaceArrow);
        workspaceBox.setOnMouseClicked(e -> showWorkspaceMenu());

        // Environment selector (right half)
        environmentBox = new HBox();
        environmentBox.getStyleClass().add("header-environment");
        environmentBox.setAlignment(Pos.CENTER);
        HBox.setHgrow(environmentBox, Priority.ALWAYS);

        environmentLabel = new Label(getEnvironmentDisplayName());
        environmentLabel.getStyleClass().add("header-environment-label");

        Label envArrow = new Label(" \u25BC");
        envArrow.getStyleClass().add("header-environment-label");

        environmentBox.getChildren().addAll(environmentLabel, envArrow);
        environmentBox.setOnMouseClicked(e -> showEnvironmentMenu());

        getChildren().addAll(workspaceBox, environmentBox);
    }

    private String getWorkspaceDisplayName() {
        Workspace ws = config.getActiveWorkspace();
        return ws != null ? ws.getName() : "No Workspace";
    }

    private String getEnvironmentDisplayName() {
        Environment env = config.getActiveEnvironment();
        return env != null ? env.getName() : "Global";
    }

    private void showWorkspaceMenu() {
        WorkspaceMenu menu = new WorkspaceMenu(config, persistence, overlayContainer, () -> {
            workspaceLabel.setText(getWorkspaceDisplayName());
            environmentLabel.setText(getEnvironmentDisplayName());
            onWorkspaceChanged.run();
        });
        menu.showBelow(workspaceBox);
    }

    private void showEnvironmentMenu() {
        EnvironmentMenu menu = new EnvironmentMenu(config, persistence, overlayContainer, () -> {
            environmentLabel.setText(getEnvironmentDisplayName());
            onEnvironmentChanged.run();
        });
        menu.showBelow(environmentBox);
    }

    public void refresh() {
        workspaceLabel.setText(getWorkspaceDisplayName());
        environmentLabel.setText(getEnvironmentDisplayName());
    }
}
