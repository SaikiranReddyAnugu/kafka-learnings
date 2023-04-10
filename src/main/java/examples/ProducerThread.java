package examples;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import proto.models.User;

import java.util.List;
import java.util.Properties;

public class ProducerThread extends Thread {
    private String topic;
    private List<User> users;
    private final Properties props;
    public ProducerThread(String topic, List<User> users,Properties props){
        this.topic = topic;
        this.users = users;
        this.props = props;
    }

    public void run() {
        try (final org.apache.kafka.clients.producer.Producer<String, User> producer = new KafkaProducer<>(props)) {
            System.out.println("inside run");
//            final Random rnd = new Random();
//            final Long numMessages = 1000000L;
            int i =1;
            for (User user : users) {
//                String userf = users[rnd.nextInt(users.length)];
//                String item = items[rnd.nextInt(items.length)];
                System.out.println(user.getFirstName() +" "+ user.getLastName()+" "+user.getMobileNumber()+ " "+topic);

                producer.send(
                        new ProducerRecord<>(topic, topic, user),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user);
                        });
                i++;
            }
            System.out.printf("%d events were produced to topic %s", i, topic);
        }
    }
}
