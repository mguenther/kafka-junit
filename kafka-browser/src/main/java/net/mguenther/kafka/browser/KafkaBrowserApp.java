package net.mguenther.kafka.browser;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.text.Font;
import javafx.stage.Stage;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.ui.MainLayout;

/**
 * Main entry point for the Kafka Topic Browser application.
 */
public class KafkaBrowserApp extends Application {

    private ConfigPersistence persistence;
    private BrowserConfig config;

    @Override
    public void start(Stage primaryStage) {
        // Load bundled JetBrains Mono font
        Font.loadFont(getClass().getResourceAsStream("/fonts/JetBrainsMono-Regular.ttf"), 12);
        Font.loadFont(getClass().getResourceAsStream("/fonts/JetBrainsMono-Bold.ttf"), 12);

        persistence = new ConfigPersistence();
        config = persistence.load();

        MainLayout mainLayout = new MainLayout(config, persistence);

        Scene scene = new Scene(mainLayout, 1280, 800);
        scene.getStylesheets().add(getClass().getResource("/styles/kafka-browser.css").toExternalForm());

        primaryStage.setTitle("Kafka Topic Browser");
        primaryStage.setScene(scene);
        primaryStage.setMinWidth(900);
        primaryStage.setMinHeight(600);
        primaryStage.show();
    }

    @Override
    public void stop() {
        if (persistence != null && config != null) {
            persistence.save(config);
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
