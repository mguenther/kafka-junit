package net.mguenther.kafka.browser.ui;

import javafx.geometry.Bounds;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Popup;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

/**
 * Custom styled dropdown menu for the environment selector.
 * Renders as a Popup with a VBox styled to match the application theme,
 * positioned directly below the environment header area.
 * Opens overlay dialogs within the provided overlay container.
 */
public class EnvironmentMenu extends Popup {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onChanged;
    private final StackPane overlayContainer;

    public EnvironmentMenu(BrowserConfig config, ConfigPersistence persistence,
                           StackPane overlayContainer, Runnable onChanged) {
        this.config = config;
        this.persistence = persistence;
        this.overlayContainer = overlayContainer;
        this.onChanged = onChanged;

        setAutoHide(true);
        setAutoFix(true);

        VBox content = buildContent();
        getContent().add(content);
    }

    private VBox buildContent() {
        VBox box = new VBox();
        box.getStyleClass().add("dropdown-menu");
        box.setPadding(new Insets(8, 0, 8, 0));
        box.setMinWidth(200);

        Workspace active = config.getActiveWorkspace();

        // Activate Environment section
        Label activateHeader = createSectionHeader("Activate Environment");
        box.getChildren().add(activateHeader);

        if (active != null) {
            for (Environment env : active.getEnvironments()) {
                Label envItem = createMenuItem("Use " + env.getName());
                envItem.setOnMouseClicked(e -> { hide(); activateEnvironment(env); });
                box.getChildren().add(envItem);
            }
        }

        Label noEnvItem = createMenuItem("No Environment");
        noEnvItem.setOnMouseClicked(e -> {
            hide();
            config.setActiveEnvironmentName(null);
            persistence.save(config);
            onChanged.run();
        });
        box.getChildren().add(noEnvItem);

        box.getChildren().add(createSeparator());

        // Manage Environments section
        Label manageHeader = createSectionHeader("Manage Environments");
        box.getChildren().add(manageHeader);

        Label editConfigItem = createMenuItem("Edit Configuration");
        editConfigItem.setOnMouseClicked(e -> { hide(); editEnvironmentConfig(); });
        box.getChildren().add(editConfigItem);

        return box;
    }

    private Label createSectionHeader(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("dropdown-section-header");
        label.setPadding(new Insets(6, 16, 2, 16));
        label.setMaxWidth(Double.MAX_VALUE);
        return label;
    }

    private Label createMenuItem(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("dropdown-menu-item");
        label.setPadding(new Insets(6, 16, 6, 16));
        label.setMaxWidth(Double.MAX_VALUE);
        label.setCursor(javafx.scene.Cursor.HAND);
        return label;
    }

    private Separator createSeparator() {
        Separator sep = new Separator();
        sep.getStyleClass().add("dropdown-separator");
        VBox.setMargin(sep, new Insets(4, 0, 4, 0));
        return sep;
    }

    public void showBelow(Node anchor) {
        Bounds bounds = anchor.localToScreen(anchor.getBoundsInLocal());
        if (bounds != null) {
            show(anchor, bounds.getMinX(), bounds.getMaxY());
        }
    }

    private void activateEnvironment(Environment env) {
        config.setActiveEnvironmentName(env.getName());
        persistence.save(config);
        onChanged.run();
    }

    private void editEnvironmentConfig() {
        Environment active = config.getActiveEnvironment();
        if (active == null) {
            Workspace ws = config.getActiveWorkspace();
            if (ws == null) return;
            active = new Environment("Global");
            ws.addEnvironment(active);
            config.setActiveEnvironmentName("Global");
        }

        final Environment envToEdit = active;
        CustomizeEnvironmentDialog dialog = new CustomizeEnvironmentDialog(envToEdit);
        dialog.setOnResult(updated -> {
            if (updated != null) {
                Workspace ws = config.getActiveWorkspace();
                if (ws != null) {
                    ws.getEnvironments().stream()
                            .filter(e -> e.getName().equals(updated.getName()))
                            .findFirst()
                            .ifPresent(existing -> existing.setParameters(updated.getParameters()));
                    persistence.save(config);
                    onChanged.run();
                }
            }
        });
        dialog.showIn(overlayContainer);
    }
}
