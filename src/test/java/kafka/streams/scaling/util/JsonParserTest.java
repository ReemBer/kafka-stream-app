package kafka.streams.scaling.util;

import kafka.streams.scaling.service.WeatherStreamService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JsonParserTest {

    @MockBean
    private WeatherStreamService weatherStreamService;

    @MockBean
    private KafkaStreams streams;

    @Autowired
    @Qualifier("valid-hotel-record-json")
    private String validHotelRecord;

    @Before
    public void setUp() {
        when(weatherStreamService.buildWeatherToHotelsMappingTopology()).thenReturn(new Topology());
        doNothing().when(streams).start();
        doNothing().when(streams).close();
    }

    @Test
    public void testParseInvalidHotelRecords() {
        assertNull(JsonParser.parseRecord(null));
        assertNull(JsonParser.parseRecord(""));
        assertNull(JsonParser.parseRecord("4g0j3459h0j340ttj"));
        assertNull(JsonParser.parseRecord("}"));
    }

    @Test
    public void parseHotelRecord() {
        final var hotelObjectNode = JsonParser.parseRecord(validHotelRecord);
        assertNotNull(hotelObjectNode);
        assertNotNull(hotelObjectNode.get("Id"));
        assertNotNull(hotelObjectNode.get("Name"));
        assertNotNull(hotelObjectNode.get("Country"));
        assertNotNull(hotelObjectNode.get("City"));
        assertNotNull(hotelObjectNode.get("Address"));
        assertNotNull(hotelObjectNode.get("Latitude"));
        assertNotNull(hotelObjectNode.get("Longitude"));
        assertNotNull(hotelObjectNode.get("geohash"));
    }
}