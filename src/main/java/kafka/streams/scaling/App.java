/**
 * A little tip:
 * 1) Weather parquet schema:
 * spark_schema {
 * optional double lat;
 * optional double lng;
 * optional double avg_tmpr_f;
 * optional double avg_tmpr_c;
 * optional binary wthr_date (UTF-8);
 * }
 * <p>
 * 2)
 */
package kafka.streams.scaling;

import kafka.streams.scaling.service.ForecastService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
@RequiredArgsConstructor
public class App implements CommandLineRunner {

    private static final Logger log = Logger.getLogger(App.class);

    private final KafkaStreams streams;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) {
        streams.setUncaughtExceptionHandler((thread, throwable) -> log.fatal("Streams Application stopped by uncaught exception.", throwable));
        addAppShutdownHandler(streams);
        streams.start();
    }

    private void addAppShutdownHandler(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                log.info("Stream stopped");
            } catch (Exception exc) {
                log.error("Got exception while executing shutdown hook: ", exc);
            }
        }));
    }
}

