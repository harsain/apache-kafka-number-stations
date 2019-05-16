package numbers;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Compute {

    private static final Logger logger = LoggerFactory.getLogger(Compute.class);

    public static final Properties config = new Properties() {
        {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "compute-radio-logs");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "numbers.MessageSerde");
        }
    };

    public static KStream<String, Message> createStream(StreamsBuilder builder) {
        // TODO: Implement me. Create a new stream with a MessageTimeExtractor for timestamps
        return builder.stream("radio-logs", Consumed.with(new MessageTimeExtractor()));
    }

    public static KStream<String, Message> filterKnown(KStream<String, Message> stream) {
        // TODO: Implement me. Filter only messages where Translator.knows(message)
        return stream.filter(new Predicate<String, Message>() {
            @Override
            public boolean test(String key, Message value) {
                return Translator.knows(value);
            }
        });
    }


    public static KStream<String, Message>[] branchSpecial(KStream<String, Message> stream) {
        // TODO: Implement me. Split the stream in two. All messages above -75 latitude, and those below (Scott Base)
        return stream.branch(
            (key, message) -> message.getLatitude() >= -75,
            (key, message) -> message.getLatitude() < -75
        );
    }

    public static void logScottBase(KStream<String, Message> stream) {
        // TODO: Implement me. Log/info each Scott Base Message
        stream.foreach((key, value) -> logger.info(String.format("Scott's Base %s", value)));
    }

    public static KStream<String, Message> translate(KStream<String, Message> stream) {
        // TODO: Implement me. Translate content from text to numeric

        return stream.mapValues(message -> {
            String translatedString = Translator.translate(message.getType(), message.getContent());
            // dont change the value reset it as in Java it will change in every branch
            return message.copy().resetContent(List.of(translatedString));
        } );
    }

    public static KTable<Windowed<String>, Message> correlate(KStream<String, Message> stream) {
        // TODO: Implement me. Correlate all messages by station name in tumbling 10 second windows

        return stream
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
            .aggregate(
                () -> null,
                (key, message, aggregation) -> {
                    if (aggregation == null) {
                        return message;
                    }
                    return aggregation.copy().addContent(message.getContent());
                },
                Materialized.as("PT10S-Store")
            );
    }

    public static void topology(StreamsBuilder builder) {
        KStream<String, Message> stream = createStream(builder);
        KStream<String, Message> filtered = filterKnown(stream);
        KStream<String, Message>[] branched = branchSpecial(filtered);
        KStream<String, Message> translated = translate(branched[0]);
        logScottBase(branched[1]);
        correlate(translated);
    }

}
