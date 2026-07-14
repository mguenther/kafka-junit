package net.mguenther.kafka.browser.ui;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.TopicInfo;

/**
 * Custom ListCell for rendering topic entries with:
 * - Topic name (bold)
 * - Partition x replica subtitle
 * - Format badge (avro/json/...)
 */
public class TopicListCell extends ListCell<TopicInfo> {

    @Override
    protected void updateItem(TopicInfo item, boolean empty) {
        super.updateItem(item, empty);
        if (empty || item == null) {
            setGraphic(null);
            setText(null);
        } else {
            HBox row = new HBox();
            row.setAlignment(Pos.CENTER_LEFT);
            row.setSpacing(10);

            VBox textBox = new VBox(2);
            HBox.setHgrow(textBox, Priority.ALWAYS);

            Label nameLabel = new Label(item.getName());
            nameLabel.getStyleClass().add("topic-name");

            Label metaLabel = new Label(
                    item.getPartitions() + (item.getPartitions() == 1 ? " Partition" : " Partitions")
                            + " x " + item.getReplicas() + (item.getReplicas() == 1 ? " replica" : " replicas"));
            metaLabel.getStyleClass().add("topic-meta");

            textBox.getChildren().addAll(nameLabel, metaLabel);

            // Format badge
            Label badge = new Label(getBadgeText(item.getFormat()));
            badge.getStyleClass().add("topic-format-badge");
            if (item.getFormat() != null) {
                badge.getStyleClass().add("topic-format-badge-" + item.getFormat());
            }

            row.getChildren().addAll(textBox, badge);
            setGraphic(row);
            setText(null);
        }
    }

    private String getBadgeText(String format) {
        if ("avro".equals(format)) return "avro";
        if ("json".equals(format)) return "json";
        return "\u2026";
    }
}
