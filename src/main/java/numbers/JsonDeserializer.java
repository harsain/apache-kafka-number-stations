package numbers;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer {

    private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);
    private Class<T> type;

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map map, boolean b) {
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        T obj = null;
        ObjectMapper mapper = new ObjectMapper();

        // TODO: Implement Me. Use the ObjectMapper to deserialize bytes into types
        try {
            obj = mapper.readValue(bytes, type);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return obj;
    }

    @Override
    public void close() {
    }
}