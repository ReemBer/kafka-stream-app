package kafka.streams.scaling;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class App implements CommandLineRunner {

    private static final Logger LOGGER = Logger.getLogger(App.class);

    private final KafkaStreams streams;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) {
        streams.setUncaughtExceptionHandler((thread, throwable) -> LOGGER.fatal("Streams Application stopped by uncaught exception.", throwable));
        addAppShutdownHandler(streams);
        streams.start();
    }

    private void addAppShutdownHandler(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                LOGGER.info("Stream stopped");
            } catch (Exception exc) {
                LOGGER.error("Got exception while executing shutdown hook: ", exc);
            }
        }));
    }
}

