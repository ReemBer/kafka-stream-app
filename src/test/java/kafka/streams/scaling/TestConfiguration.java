package kafka.streams.scaling;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static kafka.streams.scaling.util.JsonParser.parseRecord;

@Configuration
public class TestConfiguration {

    @Bean("valid-hotel-record")
    public ObjectNode getValidHotelRecord() {
        return parseRecord(getValidHotelRecordJson());
    }

    @Bean("valid-hotel-record-json")
    public String getValidHotelRecordJson() {
        return "{" +
                "\"Id\":\"3427383902209\"," +
                "\"Name\":\"H tel Barri re Le Fouquet s\"," +
                "\"Country\":\"FR\"," +
                "\"City\":\"Paris\"," +
                "\"Address\":\"46 Avenue George V 8th arr 75008 Paris France\"," +
                "\"Latitude\":\"48.8710709\"," +
                "\"Longitude\":\"2.3013119\"," +
                "\"geohash\":\"u09wh\"" +
                "}";
    }

    @Bean("valid-weather-record")
    public ObjectNode getValidWeatherRecord() {
        return parseRecord(getValidWeatherRecordJson());
    }

    @Bean("valid-weather-record-json")
    public String getValidWeatherRecordJson() {
        return "{\"lng\":-105.533,\"lat\":22.4782,\"avg_tmpr_f\":84.3,\"avg_tmpr_c\":29.1,\"wthr_date\":\"2017-08-03\",\"year\":2017,\"month\":3,\"day\":8}";
    }

    @Bean
    public ConsumerRecordFactory<String, String> getConsumerRecordFactory() {
        return new ConsumerRecordFactory<>("weather_from_hive", new StringSerializer(), new StringSerializer());
    }
}
