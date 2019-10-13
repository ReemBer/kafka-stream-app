package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
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

import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HotelServiceTest extends Assert {

    @MockBean
    private WeatherStreamService weatherStreamService;

    @MockBean
    private KafkaStreams streams;

    @MockBean
    private Consumer<String, String> hotelConsumer;

    @Autowired
    private HotelService hotelService;

    @Autowired
    @Qualifier("valid-hotel-record")
    private ObjectNode hotelRecord;

    @Before
    public void setUp() {
        when(weatherStreamService.buildWeatherToHotelsMappingTopology()).thenReturn(new Topology());
        doNothing().when(streams).start();
        doNothing().when(streams).close();
    }

    @Test
    public void testFillHotelsMapBySingleRecord() {
        //given
        final var geohash = hotelRecord.get("geohash").asText();
        final var hotelConsumerRecord = new ConsumerRecord<>("dataflow", 0, 0, "", this.hotelRecord.toString());
        final var topicPartition = new TopicPartition("dataflow", 0);
        final var recordsMap = new HashMap<TopicPartition, List<ConsumerRecord<String, String>>>();
        recordsMap.put(topicPartition, singletonList(hotelConsumerRecord));
        when(hotelConsumer.poll(anyLong())).thenReturn(new ConsumerRecords<>(recordsMap));

        //when
        final var hotelsMap = hotelService.createDictionaryWithHotelsData();

        //then
        assertEquals(3, hotelsMap.size());
        assertTrue(hotelsMap.get(geohash).contains(hotelRecord));
        assertTrue(hotelsMap.get(geohash.substring(0, 4)).contains(hotelRecord));
        assertTrue(hotelsMap.get(geohash.substring(0, 3)).contains(hotelRecord));
    }

    /**
     * If two hotels placed near then we expect their geohashes have the same prefix.
     * Therefore, such hotels will be placed to {@code hotelsMap} with the same key.
     */
    @Test
    public void testDictionaryCreationWithNeighbouringHotels() {
        //given
        final var hotel = hotelRecord;
        final var neighbourGeohash = hotelRecord.get("geohash").asText().substring(0, 3) + "00";
        final var neighbourHotel = hotelRecord.deepCopy();
        neighbourHotel.put("geohash", neighbourGeohash);

        final var hotelConsumerRecord = new ConsumerRecord<>("dataflow", 0, 0, "", this.hotelRecord.toString());
        final var neighbourHotelConsumerRecord = new ConsumerRecord<>("dataflow", 0, 0, "", neighbourHotel.toString());
        final var topicPartition = new TopicPartition("dataflow", 0);

        final var recordsMap = new HashMap<TopicPartition, List<ConsumerRecord<String, String>>>();
        recordsMap.put(topicPartition, asList(hotelConsumerRecord, neighbourHotelConsumerRecord));

        when(hotelConsumer.poll(anyLong())).thenReturn(new ConsumerRecords<>(recordsMap));

        //when
        final var hotelsMap = hotelService.createDictionaryWithHotelsData();
        final var hotelsWithThreeDigitsPrecision = hotelsMap.get(neighbourGeohash.substring(0, 3));
        final var hotelsWithFourDigitsPrecision = hotelsMap.get(neighbourGeohash.substring(0, 4));
        final var hotelsWithFiveDigitsPrecision = hotelsMap.get(neighbourGeohash);

        //then
        assertEquals(2, hotelsWithThreeDigitsPrecision.size());
        assertTrue(hotelsWithThreeDigitsPrecision.contains(hotelRecord));
        assertTrue(hotelsWithThreeDigitsPrecision.contains(neighbourHotel));

        assertEquals(1, hotelsWithFourDigitsPrecision.size());
        assertTrue(hotelsWithFourDigitsPrecision.contains(neighbourHotel));
        assertFalse(hotelsWithFourDigitsPrecision.contains(hotelRecord));

        assertEquals(1, hotelsWithFiveDigitsPrecision.size());
        assertTrue(hotelsWithFourDigitsPrecision.contains(neighbourHotel));
        assertFalse(hotelsWithFourDigitsPrecision.contains(hotelRecord));
    }
}
