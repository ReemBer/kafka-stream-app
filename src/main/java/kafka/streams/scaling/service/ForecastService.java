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

@Service
@RequiredArgsConstructor
public class ForecastService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WEATHER_INPUT_TOPIC_NAME = "weather_from_hive";
    private static final int N_DIGITS_PRECISION = 5;

    private final HotelService hotelService;

    public Topology buildForecastProducingTopology() {
        final var hotelMap = hotelService.createDictionaryWithHotelsData();
        final var builder = new StreamsBuilder();
        builder
                .stream(WEATHER_INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, jsonStringWeather) -> new KeyValue<>(key, getWeatherObjectNode(jsonStringWeather)))
                .filter((key, weather) -> Objects.nonNull(weather))
                .flatMap((key, weather) -> {
                    enrichWeatherWithGeoHash(weather);
                    final var mappedHotels = mapWeatherToHotelsByGeoHash(weather, hotelMap);
                    return createCurrentPieceOfForecastDataSet(key, weather, mappedHotels);
                })
                .to("hotels_forecast", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

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

    private void enrichWeatherWithGeoHash(final ObjectNode weather) {
        weather.put(
                "geohash",
                geoHashStringWithCharacterPrecision(weather.get("lat").asDouble(), weather.get("lng").asDouble(), N_DIGITS_PRECISION)
        );
    }

    private KeyValue<String, String> createForecastRecord(final String key,
                                                          final ObjectNode weather,
                                                          final int precision,
                                                          final ObjectNode hotel) {
        var forecastForHotelPerDay = MAPPER.createObjectNode();
        forecastForHotelPerDay.put("weather", weather);
        forecastForHotelPerDay.put("hotel", hotel);
        forecastForHotelPerDay.put("precision", precision);
        return new KeyValue<String, String>(key, forecastForHotelPerDay.toString());
    }

    private ObjectNode getWeatherObjectNode(String jsonStringWeather) {
        try {
            return (ObjectNode) MAPPER.readTree(jsonStringWeather);
        } catch (Exception e) {
            return null;
        }
    }
}
