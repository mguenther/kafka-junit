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
 */
public class IconSidebar extends VBox {

    public IconSidebar(Runnable onSearch, Runnable onBrowse, Runnable onInfo) {
        getStyleClass().add("icon-sidebar");
        setPrefWidth(45);
        setMinWidth(45);
        setMaxWidth(45);
        setAlignment(Pos.TOP_CENTER);
        setPadding(new Insets(10, 0, 10, 0));
        setSpacing(5);

        Button searchBtn = createIconButton("\uD83D\uDD0D", "Search", onSearch);
        Button browseBtn = createIconButton("\uD83D\uDD16", "Browse Topics", onBrowse);
        Button infoBtn = createIconButton("\u2139", "Info", onInfo);

        getChildren().addAll(searchBtn, browseBtn, infoBtn);
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
