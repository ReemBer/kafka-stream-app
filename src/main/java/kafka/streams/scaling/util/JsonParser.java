package kafka.streams.scaling.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import org.apache.log4j.Logger;

import static java.util.Objects.isNull;

public class JsonParser {

    private static final Logger LOGGER = Logger.getLogger(JsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parse valid json string to {@link ObjectNode}.
     *
     * @param json json string.
     * @return {@link ObjectNode} if passed {@code json} is valid json string. Else returns null.
     */
    public static ObjectNode parseRecord(final String json) {
        try {
            return (ObjectNode) (isNull(json) ? null : MAPPER.readTree(json));
        } catch (IOException e) {
            LOGGER.error("Unable to parse record : " + json);
        }
        return null;
    }
}
