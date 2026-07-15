package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;

import java.util.function.Consumer;

/**
 * Base class for in-app overlay dialogs that render as centered panels
 * within the main content area, matching the application's visual style.
 * These replace JavaFX's Dialog/Alert which use native OS chrome.
 */
public abstract class OverlayDialog<T> extends StackPane {

    protected final VBox dialogPane;
    private Consumer<T> onResult;
    private boolean modal = true;

    public OverlayDialog() {
        // Semi-transparent background overlay
        getStyleClass().add("overlay-backdrop");
        setAlignment(Pos.CENTER);

        // Dialog panel
        dialogPane = new VBox();
        dialogPane.getStyleClass().add("overlay-dialog");
        dialogPane.setPadding(new Insets(20, 30, 20, 30));
        dialogPane.setMaxWidth(600);
        dialogPane.setMinWidth(400);
        dialogPane.setMaxHeight(javafx.scene.layout.Region.USE_PREF_SIZE);

        getChildren().add(dialogPane);

        // Close on backdrop click (unless modal)
        setOnMouseClicked(e -> {
            if (e.getTarget() == this && !modal) {
                close(null);
            }
        });
    }

    /**
     * Sets whether this dialog is modal (cannot be dismissed by clicking the backdrop).
     * When modal, the user must use an explicit close/cancel button.
     */
    protected void setModal(boolean modal) {
        this.modal = modal;
    }

    /**
     * Sets the callback invoked when the dialog produces a result (or is cancelled).
     */
    public void setOnResult(Consumer<T> onResult) {
        this.onResult = onResult;
    }

    /**
     * Closes the dialog, removing it from its parent, and notifies the result callback.
     */
    protected void close(T result) {
        Node parent = getParent();
        if (parent instanceof StackPane sp) {
            sp.getChildren().remove(this);
        }
        if (onResult != null) {
            onResult.accept(result);
        }
    }

    /**
     * Shows the dialog by adding it as an overlay on top of the given container.
     * The container should be a StackPane (like the content area).
     */
    public void showIn(StackPane container) {
        if (!container.getChildren().contains(this)) {
            container.getChildren().add(this);
        }
        toFront();
        requestFocus();
    }
}
