package examples;

import dto.Data;
import org.apache.kafka.clients.producer.*;
import util.DummyDataUtil;


import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ProducerExample extends Thread{




    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }
        List<Data> data = DummyDataUtil.getSampleData();

        // Load producer configuration settings from a local file
        final Properties props = loadConfig(args[0]);
        for(Data eachProducer : data){
            ProducerThread p = new ProducerThread(eachProducer.getTopicName(),eachProducer.getUsers(), eachProducer.getItems(), props);
            p.start();
        }

//        Producer1(props);
//        Producer2(props);
//        Producer3(props);
//        Producer4(props);
//        Producer5(props);

    }



//
//    public static void Producer5(Properties props){
//        final String topic = "purchases";
//
//        String[] users = {"eabara5", "jsmith5", "sgarcia5", "jbernard5", "htanaka5", "awalther5"};
//        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
//        ProduceData(props, users, items, topic);
//    }



//    public static void ProduceData(Properties props,String[] users,String[] items, String topic){
//        try (final org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
//            final Random rnd = new Random();
//            final Long numMessages = 10L;
//            for (Long i = 0L; i < numMessages; i++) {
//                String user = users[rnd.nextInt(users.length)];
//                String item = items[rnd.nextInt(items.length)];
//                System.out.println(i);
//
//                producer.send(
//                        new ProducerRecord<>(topic, user, item),
//                        (event, ex) -> {
//                            if (ex != null)
//                                ex.printStackTrace();
//                            else
//                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
//                        });
//            }
//            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
//        }
//    }

    // We'll reuse this function to load properties from the Consumer as well
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}