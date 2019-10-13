package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import kafka.streams.scaling.util.JsonParser;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static java.util.stream.StreamSupport.stream;

@Service
@RequiredArgsConstructor
public class HotelService {

    private static final Logger LOGGER = Logger.getLogger(HotelService.class);

    private static final int CONSUMER_POLLING_DATA_ATTEMPTS_NUMBER = 50;

    private final Consumer<String, String> hotelConsumer;

    /**
     * Retrieves hotels data from kafka topic, parses it and creates dictionary of it in {@link Multimap}.
     *
     * @return A {@link Multimap} instance with retrieved and parsed hotels data.
     */
    public Multimap<String, ObjectNode> createDictionaryWithHotelsData() {
        int attemptsNumber = 1;
        do {
            final var hotelRecords = hotelConsumer.poll(1000);
            if (hotelRecords.isEmpty()) {
                LOGGER.warn("Unable to retrieve data from topic in 1000 millisecond. Number of attempts : " + attemptsNumber);
                continue;
            }
            hotelConsumer.close();
            return getHotelsRecordMultimap(hotelRecords);
        } while (attemptsNumber++ <= CONSUMER_POLLING_DATA_ATTEMPTS_NUMBER);
        return HashMultimap.create(0, 0);
    }

    /**
     * Creates {@link Multimap} and puts retrieved hotels records in this multimap.
     * Pairs have {@code (hotel_geohash, hotel)} format.
     * We need to use {@link Multimap} because we want to map geohash with the different precision to one hotel.
     * We can face the situation when several hotels have the same prefix of their geohash and, therefore, the same key in map.
     *
     * @param hotelRecords records retrieved from kafka topic
     * @return A {@link Multimap} object with parsed hotel records
     */
    private Multimap<String, ObjectNode> getHotelsRecordMultimap(final ConsumerRecords<String, String> hotelRecords) {
        final var hotelsMap = HashMultimap.<String, ObjectNode>create();
        stream(hotelRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .map(JsonParser::parseRecord)
                .filter(Objects::nonNull)
                .forEach(hotel -> putHotelRecordToMap(hotelsMap, hotel));
        return hotelsMap;
    }

    /**
     * Puts single hotel record to {@code hotelsMap} with several keys.
     * Each key is the {@code geohash} of hotel record with 5 chars, 4 chars and 3 chars respectively.
     *
     * @param hotelsMap {@link Multimap} with hotels records.
     * @param hotel record to be inserted to {@code hotelsMap}.
     */
    private void putHotelRecordToMap(final HashMultimap<String, ObjectNode> hotelsMap, final ObjectNode hotel) {
        var geohash = hotel.get("geohash").asText();
        hotelsMap.put(geohash, hotel);
        hotelsMap.put(geohash.substring(0, 4), hotel);
        hotelsMap.put(geohash.substring(0, 3), hotel);
    }
}
