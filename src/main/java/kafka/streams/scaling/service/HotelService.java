package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Objects;

import static java.util.stream.StreamSupport.stream;

@Service
@RequiredArgsConstructor
public class HotelService {

    private static final Logger LOGGER = Logger.getLogger(HotelService.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CONSUMER_POLLING_DATA_ATTEMPTS_NUMBER = 50;

    private final Consumer<String, String> hotelConsumer;

    public Multimap<String, ObjectNode> createDictionaryWithHotelsData() {
        int attemptsNumber = 1;
        do {
            final var hotelRecords = hotelConsumer.poll(1000);
            if (hotelRecords.isEmpty()) {
                LOGGER.warn("Unable to retrieve data from topic in 1000 millisecond. Number of attempts : " + attemptsNumber);
                continue;
            }
            hotelConsumer.close();
            return getHotelsRecordToMultimap(hotelRecords);
        } while (attemptsNumber++ <= CONSUMER_POLLING_DATA_ATTEMPTS_NUMBER);
        return HashMultimap.create(0, 0);
    }

    /**
     * Creates {@link Multimap} and puts retrieved hotels records in this multimap.
     * Pairs have {@code (hotel_geohash, hotel)} format.
     * We need to use {@link Multimap} because we want to map geohash with the different precision to one hotel.
     * We can face the situation when several hotels have the same prefix of their geohash and, therefore, the same key in map.
     *
     * @param hotelRecords
     * @return
     */
    private Multimap<String, ObjectNode> getHotelsRecordToMultimap(final ConsumerRecords<String, String> hotelRecords) {
        final var hotelsMap = HashMultimap.<String, ObjectNode>create();
        stream(hotelRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .map(this::parseHotelRecord)
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
    public void putHotelRecordToMap(final HashMultimap<String, ObjectNode> hotelsMap, final ObjectNode hotel) {
        var geohash = hotel.get("geohash").asText();
        hotelsMap.put(geohash, hotel);
        hotelsMap.put(geohash.substring(0, 4), hotel);
        hotelsMap.put(geohash.substring(0, 3), hotel);
    }

    /**
     * Parse valid json string to {@link ObjectNode}. Expected, that passed string contains hotel record.
     *
     * @param hotelStringValue json string with hotel record.
     * @return {@link ObjectNode} if passed {@code hotelStringValue} is valid json string. Else returns null.
     */
    public ObjectNode parseHotelRecord(final String hotelStringValue) {
        try {
            return (ObjectNode) MAPPER.readTree(hotelStringValue);
        } catch (IOException e) {
            LOGGER.error("Unable to parse hotel record : " + hotelStringValue);
        }
        return null;
    }
}
