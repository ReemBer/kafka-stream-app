package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import kafka.streams.scaling.util.JsonParser;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static kafka.streams.scaling.service.WeatherStreamService.N_DIGITS_PRECISION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * Test class for testing Kafka Stream pipeline.
 * It tests {@link org.apache.kafka.streams.Topology} used in application without external Kafka Cluster.
 * To do that, it uses {@link TopologyTestDriver} and {@link ConsumerRecordFactory}.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class WeatherStreamServiceTest {

    @MockBean
    private KafkaStreams streams;

    @MockBean
    private HotelService hotelService;


    @Autowired
    private WeatherStreamService weatherStreamService;

    @Autowired
    @Qualifier("kafka-stream-connection-config")
    private Properties properties;

    @Autowired
    @Qualifier("valid-weather-record")
    private ObjectNode weatherRecord;

    @Autowired
    @Qualifier("valid-hotel-record")
    private ObjectNode hotelRecord;

    @Autowired
    private ConsumerRecordFactory<String, String> consumerRecordFactory;

    @Before
    public void buildWeatherToHotelsMappingTopology() {
        doNothing().when(streams).start();
        doNothing().when(streams).close();
    }

    @Test
    public void testGeohashOnWeatherRecord() {
        //given
        final var weatherLat = weatherRecord.get("lat").asDouble();
        final var weatherLng = weatherRecord.get("lng").asDouble();
        final var geohash = geoHashStringWithCharacterPrecision(weatherLat, weatherLng, N_DIGITS_PRECISION);
        hotelRecord.put("geohash", geohash);
        final var hotelsMultimap = HashMultimap.<String, ObjectNode>create();
        hotelsMultimap.put(hotelRecord.get("geohash").asText(), hotelRecord);
        when(hotelService.createDictionaryWithHotelsData()).thenReturn(hotelsMultimap);
        final var weatherTopologyTestDriver = new TopologyTestDriver(weatherStreamService.buildWeatherToHotelsMappingTopology(), properties);

        //when
        weatherTopologyTestDriver.pipeInput(consumerRecordFactory.create(weatherRecord.toString()));
        final var forecastRecord = Optional
                .ofNullable(weatherTopologyTestDriver.readOutput("hotels_forecast", new StringDeserializer(), new StringDeserializer()))
                .map(ProducerRecord::value)
                .map(JsonParser::parseRecord)
                .orElse(null);

        //then
        assertNotNull(forecastRecord);
        final var weatherPartOfOutput = forecastRecord.get("weather");
        assertThat(weatherPartOfOutput.get("geohash").asText()).isEqualTo(geohash);
    }

    @Test
    public void testPrecision() {
        //given
        final var weatherLat = weatherRecord.get("lat").asDouble();
        final var weatherLng = weatherRecord.get("lng").asDouble();
        final var geohash = geoHashStringWithCharacterPrecision(weatherLat, weatherLng, N_DIGITS_PRECISION);

        hotelRecord.put("geohash", geohash);
        final var hotelRecordWith4Precision = hotelRecord.deepCopy();
        hotelRecordWith4Precision.put("geohash", "9suu0");
        final var hotelRecordWith3Precision = hotelRecord.deepCopy();
        hotelRecordWith3Precision.put("geohash", "9tu11");
        final var weatherWith4Precision = weatherRecord.deepCopy();
        weatherWith4Precision.put("lat", weatherLat + 5);
        final var weatherWith3Precision = weatherRecord.deepCopy();
        weatherWith3Precision.put("lat", weatherLat + 10);

        final var hotelsMultimap = HashMultimap.<String, ObjectNode>create();
        hotelsMultimap.put(hotelRecord.get("geohash").asText(), hotelRecord);
        hotelsMultimap.put(hotelRecord.get("geohash").asText().substring(0, 4), hotelRecord);
        hotelsMultimap.put(hotelRecord.get("geohash").asText().substring(0, 3), hotelRecord);
        hotelsMultimap.put(hotelRecordWith4Precision.get("geohash").asText(), hotelRecordWith4Precision);
        hotelsMultimap.put(hotelRecordWith4Precision.get("geohash").asText().substring(0, 4), hotelRecordWith4Precision);
        hotelsMultimap.put(hotelRecordWith4Precision.get("geohash").asText().substring(0, 3), hotelRecordWith4Precision);
        hotelsMultimap.put(hotelRecordWith3Precision.get("geohash").asText(), hotelRecordWith3Precision);
        hotelsMultimap.put(hotelRecordWith3Precision.get("geohash").asText().substring(0, 4), hotelRecordWith3Precision);
        hotelsMultimap.put(hotelRecordWith3Precision.get("geohash").asText().substring(0, 3), hotelRecordWith3Precision);

        when(hotelService.createDictionaryWithHotelsData()).thenReturn(hotelsMultimap);
        final var weatherTopologyTestDriver = new TopologyTestDriver(weatherStreamService.buildWeatherToHotelsMappingTopology(), properties);

        //when
        weatherTopologyTestDriver.pipeInput(
                of(weatherRecord, weatherWith4Precision, weatherWith3Precision)
                        .map(Object::toString)
                        .map(consumerRecordFactory::create)
                        .collect(toList())
        );
        final var forecastRecords = range(0, 4)
                .mapToObj(i -> weatherTopologyTestDriver.readOutput("hotels_forecast", new StringDeserializer(), new StringDeserializer()))
                .filter(Objects::nonNull)
                .map(ProducerRecord::value)
                .map(JsonParser::parseRecord)
                .collect(toList());

        //then
        assertEquals(3, forecastRecords.size());
        assertEquals(5, forecastRecords.get(0).get("precision").asInt());
        assertEquals(4, forecastRecords.get(1).get("precision").asInt());
        assertEquals(3, forecastRecords.get(2).get("precision").asInt());
    }
}