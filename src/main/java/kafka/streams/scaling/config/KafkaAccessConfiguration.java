package kafka.streams.scaling.config;

import kafka.streams.scaling.service.WeatherStreamService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;
import java.util.Properties;

import static java.util.Collections.singleton;

@Configuration
public class KafkaAccessConfiguration {

    private static final String DEFAULT_BOOTSTRAP_SERVER = "sandbox-hdp.hortonworks.com:6667";
    private static final String DEFAULT_HOTELS_TOPIC = "dataflow";

    private static final String APPLICATION_ID_CONFIG = "weather-consumer-group";
    private static final String HOTELS_GROUP_ID = "hotels-consumer-group";
    private static final String BOOTSTRAP_SERVERS_CONFIG_ENV_VAR = "BOOTSTRAP_SERVERS_CONFIG";

    @Bean(name = "kafka-stream-connection-config")
    public Properties getKafkaStreamsConnectionConfig() {
        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return config;
    }

    @Bean
    public Consumer<String, String> getHotelConsumer() {
        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, HOTELS_GROUP_ID);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * 3);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.MAX_VALUE);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final var consumer = new KafkaConsumer<String, String>(config);
        consumer.subscribe(singleton(DEFAULT_HOTELS_TOPIC));
        return consumer;
    }

    @Bean
    @Autowired
    public KafkaStreams getWeatherKstreamHandler(final WeatherStreamService weatherStreamService,
                                                 final @Qualifier("kafka-stream-connection-config") Properties streamConsumerConfig) {
        return new KafkaStreams(weatherStreamService.buildWeatherToHotelsMappingTopology(), streamConsumerConfig);
    }

    private static String getBootstrapServers() {
        return Optional.ofNullable(System.getenv(BOOTSTRAP_SERVERS_CONFIG_ENV_VAR)).orElse(DEFAULT_BOOTSTRAP_SERVER);
    }
}
