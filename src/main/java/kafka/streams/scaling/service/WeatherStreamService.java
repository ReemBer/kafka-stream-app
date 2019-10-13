package kafka.streams.scaling.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Multimap;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision;
import static java.util.stream.Collectors.toList;
import static kafka.streams.scaling.util.JsonParser.parseRecord;

@Service
@RequiredArgsConstructor
public class WeatherStreamService {

    public static final int N_DIGITS_PRECISION = 5;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WEATHER_INPUT_TOPIC_NAME = "weather_from_hive";

    private final HotelService hotelService;

    /**
     * Builds the {@link Topology} instance to retrieve weather data, map it to hotels data, and write result to the other Kafka topic.
     *
     * @return {@link Topology} instance.
     */
    public Topology buildWeatherToHotelsMappingTopology() {
        final var hotelMap = hotelService.createDictionaryWithHotelsData();
        final var builder = new StreamsBuilder();
        builder
                .stream(WEATHER_INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, jsonStringWeather) -> new KeyValue<>(key, parseRecord(jsonStringWeather)))
                .filter((key, weather) -> Objects.nonNull(weather))
                .flatMap((key, weather) -> {
                    enrichWeatherWithGeoHash(weather);
                    final var mappedHotels = mapWeatherToHotelsByGeoHash(weather, hotelMap);
                    return createCurrentPieceOfForecastDataSet(key, weather, mappedHotels);
                })
                .to("hotels_forecast", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    /**
     * Create instances of records of the final data set (named forecast) by mapped {@code hotel} instances to current {@code weather} instance.
     * @param key unused parameter. Need just for api compatibility
     * @param weather weather record retrieved from kafka topic and parsed.
     * @param mappedHotels {@code hotels} matched by geohash to current {@code weather} instance.
     * @return The {@link List} of the final data set records for current {@code weather} record.
     */
    private List<KeyValue<String, String>> createCurrentPieceOfForecastDataSet(final String key,
                                                                               final ObjectNode weather,
                                                                               final Pair<Collection<ObjectNode>, Integer> mappedHotels) {
        final var matchedHotels = mappedHotels.getLeft();
        final var precision = mappedHotels.getRight();
        return matchedHotels
                .stream()
                .map(hotel -> createForecastRecord(key, weather, precision, hotel))
                .collect(toList());
    }

    /**
     * Finds closest {@code hotels} to current {@code weather} record using {@code geohash} field.
     * It tries firstly map hotels by 5 chars precision. In case of no match it uses 4 or 3 chars.
     * Finally, it adds {@code precision} to result.
     *
     * @param weather weather record retrieved from kafka topic and parsed.
     * @param hotelMap {@link Multimap} instance containing hotels retrieved from kafka topic and parsed.
     * @return {@link Pair} instance with matched by geohash {@code hotels} and used {@code precision}
     */
    private Pair<Collection<ObjectNode>, Integer> mapWeatherToHotelsByGeoHash(final ObjectNode weather,
                                                                              final Multimap<String, ObjectNode> hotelMap) {
        final var geohash = weather.get("geohash").asText();
        var matchedHotels = hotelMap.get(geohash);
        var usedPrecision = 5;
        if (matchedHotels.isEmpty()) {
            matchedHotels = hotelMap.get(geohash.substring(0, 4));
            usedPrecision = 4;
            if (matchedHotels.isEmpty()) {
                matchedHotels = hotelMap.get(geohash.substring(0, 3));
                usedPrecision = 3;
            }
        }
        return Pair.of(matchedHotels, usedPrecision);
    }

    /**
     * Computes and adds geohash to the {@code weather} record.
     *
     * @param weather weather record retrieved from kafka topic and parsed.
     */
    private void enrichWeatherWithGeoHash(final ObjectNode weather) {
        weather.put(
                "geohash",
                geoHashStringWithCharacterPrecision(weather.get("lat").asDouble(), weather.get("lng").asDouble(), N_DIGITS_PRECISION)
        );
    }

    /**
     * Creates record of the final data set (named forecast) with weater per day/hotel with precision.
     *
     * @param key unused parameter. Need just for api compatibility
     * @param weather weather record retrieved from kafka topic and parsed.
     * @param precision {@code weather} record mapped to {@code hotel} record with this precision.
     * @param hotel hotel record retrieved from kafka topic and parsed.
     * @return {@link KeyValue} instance with forecast record.
     */
    private KeyValue<String, String> createForecastRecord(final String key,
                                                          final ObjectNode weather,
                                                          final int precision,
                                                          final ObjectNode hotel) {
        var forecastForHotelPerDay = MAPPER.createObjectNode();
        forecastForHotelPerDay.put("weather", weather);
        forecastForHotelPerDay.put("hotel", hotel);
        forecastForHotelPerDay.put("precision", precision);
        return new KeyValue<>(key, forecastForHotelPerDay.toString());
    }
}
