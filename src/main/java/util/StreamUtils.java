package util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;

public class StreamUtils {
//    final Map<String, String> serdeConfig = Map.of(BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO",
//            SCHEMA_REGISTRY_URL_CONFIG,"https://psrc-l6o18.us-east-2.aws.confluent.cloud",
//            USER_INFO_CONFIG,"NONCHQ4HJEBNWB5N:+EJ4qPgIh/sqw7WjiiyBpVaSmBEkX3qT1zoguiymUzoidpc+xf+HQUmlKWh+tUyA");

//    KafkaProtobufSerde<User> protobufSerde = new KafkaProtobufSerde<>(User.class);
//            protobufSerde.configure(serdeConfig, false);

    public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String)key, (String)value));
        return configs;
    }

}
