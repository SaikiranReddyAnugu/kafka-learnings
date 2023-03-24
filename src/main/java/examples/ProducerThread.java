package examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProducerThread extends Thread {
    private String topic;
    private String[] users;
    private String[] items;
    private final Properties props;
    public ProducerThread(String topic, String[] users, String[] items, Properties props){
        this.topic = topic;
        this.users = users;
        this.items = items;
        this.props = props;
    }

    public void run() {
        try (final org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final Long numMessages = 1000000L;
            for (Long i = 0L; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)];
                System.out.println(i + topic);

                producer.send(
                        new ProducerRecord<>(topic, user, item),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }
    }
}
