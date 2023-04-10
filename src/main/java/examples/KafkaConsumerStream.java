package examples;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import proto.models.User;
import util.StreamUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
public class KafkaConsumerStream {

    public static void main(String[] args) throws IOException {
        // configure Kafka Streams properties
        try{
        final Properties props = ProducerExample.loadConfig(args[0]);
//            final Map<String, String> serdeConfig = Map.of(BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO",
//                    SCHEMA_REGISTRY_URL_CONFIG,"https://psrc-l6o18.us-east-2.aws.confluent.cloud",
//                    USER_INFO_CONFIG,"NONCHQ4HJEBNWB5N:+EJ4qPgIh/sqw7WjiiyBpVaSmBEkX3qT1zoguiymUzoidpc+xf+HQUmlKWh+tUyA");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-kafka-streams-app");

        final Map<String, Object> configMap = StreamUtils.propertiesToMap(props);
        KafkaProtobufSerde<User> protobufSerde = new KafkaProtobufSerde<>(User.class);
        protobufSerde.configure(configMap, false);
        // create a stream builder
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println("test6");

        // create a stream from a Kafka topic
        KStream<String, User> input = builder.stream("test6", Consumed.with(Serdes.String(), protobufSerde));

        input.print(Printed.<String, User>toSysOut().withLabel("values-test"));

        try {
                KStream<String, User>  output = input.mapValues(value -> {
                    return User.newBuilder()
                .setFirstName(value.getFirstName().substring(2))
                            .setLastName(value.getLastName())
                            .setMobileNumber(value.getMobileNumber()).build();
                });
                output.to("test5", Produced.with(Serdes.String(), protobufSerde));
            }
            catch(Exception e){
                System.out.println(e);
            }
        // write the output to a Kafka topic
        System.out.println("inside4" + input);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();

    }catch(Exception e){
            System.out.println("exception : " + e);
        }
    }

}
