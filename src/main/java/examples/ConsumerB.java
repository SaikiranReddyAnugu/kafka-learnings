package examples;

import org.apache.kafka.clients.consumer.*;
import proto.models.User;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerB {

    public static void main(final String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        final String topic = "test2";

        // Load consumer configuration settings from a local file
        // Reusing the loadConfig method from the ProducerExample class
        final Properties props = ProducerExample.loadConfig(args[0]);
        int i=0;
        // Add additional properties.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("derive.type", "true");


        try (final org.apache.kafka.clients.consumer.Consumer<String, User> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            System.out.println("total values " + i);
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
                int j=0;
                for (ConsumerRecord<String, User> record : records) {
                    String key = record.key();
                    User value = record.value();
                    j++;
                    System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
                i= i+j;
            }
        }
    }

}