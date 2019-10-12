package kafka.streams.scaling;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfiguration {

    @Bean("valid-hotel-record-json")
    public String getValidHotelRecord() {
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
}
