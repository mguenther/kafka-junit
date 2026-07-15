package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.VBox;

/**
 * Narrow icon sidebar on the left with navigation buttons:
 * - Search (magnifying glass)
 * - Browse/Bookmark
 * - Info
 *
 * Supports:
 * - Active tab highlighting (the currently selected view)
 * - Disabled state when no workspace is configured
 */
public class IconSidebar extends VBox {

    private Button searchBtn;
    private Button browseBtn;
    private Button infoBtn;
    private Button activeButton;

    public IconSidebar(Runnable onSearch, Runnable onBrowse, Runnable onInfo) {
        getStyleClass().add("icon-sidebar");
        setPrefWidth(45);
        setMinWidth(45);
        setMaxWidth(45);
        setAlignment(Pos.TOP_CENTER);
        setPadding(new Insets(10, 0, 10, 0));
        setSpacing(5);

        searchBtn = createIconButton("\uD83D\uDD0D", "Search", () -> {
            setActive(searchBtn);
            onSearch.run();
        });
        browseBtn = createIconButton("\uD83D\uDD16", "Browse Topics", () -> {
            setActive(browseBtn);
            onBrowse.run();
        });
        infoBtn = createIconButton("\u2139", "Info", () -> {
            setActive(infoBtn);
            onInfo.run();
        });

        getChildren().addAll(browseBtn, searchBtn, infoBtn);

        // Default: browse is active
        setActive(browseBtn);
    }

    /**
     * Sets the active (highlighted) button in the sidebar.
     */
    private void setActive(Button button) {
        if (activeButton != null) {
            activeButton.getStyleClass().remove("icon-sidebar-button-active");
        }
        activeButton = button;
        if (activeButton != null && !activeButton.getStyleClass().contains("icon-sidebar-button-active")) {
            activeButton.getStyleClass().add("icon-sidebar-button-active");
        }
    }

    /**
     * Enables or disables all sidebar buttons.
     * When disabled, buttons are visually dimmed and non-interactive.
     */
    public void setButtonsDisabled(boolean disabled) {
        searchBtn.setDisable(disabled);
        browseBtn.setDisable(disabled);
        infoBtn.setDisable(disabled);
    }

    /**
     * Programmatically activates the browse tab.
     */
    public void activateBrowse() {
        setActive(browseBtn);
    }

    /**
     * Programmatically activates the search tab.
     */
    public void activateSearch() {
        setActive(searchBtn);
    }

    private Button createIconButton(String icon, String tooltip, Runnable action) {
        Button btn = new Button(icon);
        btn.getStyleClass().add("icon-sidebar-button");
        btn.setTooltip(new Tooltip(tooltip));
        btn.setOnAction(e -> action.run());
        btn.setPrefSize(35, 35);
        return btn;
    }
}
