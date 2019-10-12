package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HotelServiceTest extends Assert {

    @MockBean
    private ForecastService forecastService;

    @MockBean
    private KafkaStreams streams;

    @Autowired
    private HotelService hotelService;

    @Autowired
    @Qualifier("valid-hotel-record-json")
    private String validHotelRecord;

    @Before
    public void setUp() {
        when(forecastService.buildForecastProducingTopology()).thenReturn(new Topology());
        doNothing().when(streams).start();
    }

    @Test
    public void testParseInvalidHotelRecords() {
        assertNull(hotelService.parseHotelRecord(""));
        assertNull(hotelService.parseHotelRecord("4g0j3459h0j340ttj"));
        assertNull(hotelService.parseHotelRecord("}"));
    }

    @Test
    public void testParseValidHotelRecords() {
        final var hotelObjectNode = hotelService.parseHotelRecord(validHotelRecord);
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

    @Test
    public void testFillHotelsMapBySingleRecord() {
        final var hotelsMap = HashMultimap.<String, ObjectNode>create();
        final var hotel = hotelService.parseHotelRecord(validHotelRecord);
        final var geohash = hotel.get("geohash").asText();

        hotelService.putHotelRecordToMap(hotelsMap, hotel);

        assertEquals(3, hotelsMap.size());
        assertTrue(hotelsMap.get(geohash).contains(hotel));
        assertTrue(hotelsMap.get(geohash.substring(0, 4)).contains(hotel));
        assertTrue(hotelsMap.get(geohash.substring(0, 3)).contains(hotel));
    }

    /**
     * If two hotels placed near then we expect their geohashes have the same prefix.
     * Therefore, such hotels will be placed to {@code hotelsMap} with the same key.
     */
    @Test
    public void testFillHotelsMapByNeighbouringHotels() {
        final var hotelsMap = HashMultimap.<String, ObjectNode>create();
        final var hotel = hotelService.parseHotelRecord(validHotelRecord);
        final var neighbourGeohash = hotel.get("geohash").asText().substring(0, 3) + "00";
        final var neighbourHotel = hotel.deepCopy();
        neighbourHotel.put("geohash", neighbourGeohash);
        hotelService.putHotelRecordToMap(hotelsMap, hotel);
        hotelService.putHotelRecordToMap(hotelsMap, neighbourHotel);

        final var hotelsWithThreeDigitsPrecision = hotelsMap.get(neighbourGeohash.substring(0, 3));
        final var hotelsWithFourDigitsPrecision = hotelsMap.get(neighbourGeohash.substring(0, 4));
        final var hotelsWithFiveDigitsPrecision = hotelsMap.get(neighbourGeohash);

        assertEquals(2, hotelsWithThreeDigitsPrecision.size());
        assertTrue(hotelsWithThreeDigitsPrecision.contains(hotel));
        assertTrue(hotelsWithThreeDigitsPrecision.contains(neighbourHotel));

        assertEquals(1, hotelsWithFourDigitsPrecision.size());
        assertTrue(hotelsWithFourDigitsPrecision.contains(neighbourHotel));
        assertFalse(hotelsWithFourDigitsPrecision.contains(hotel));

        assertEquals(1, hotelsWithFiveDigitsPrecision.size());
        assertTrue(hotelsWithFourDigitsPrecision.contains(neighbourHotel));
        assertFalse(hotelsWithFourDigitsPrecision.contains(hotel));
    }
}
