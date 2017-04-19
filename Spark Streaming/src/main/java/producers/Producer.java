package producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {


        String topicName = "messages";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", 65536);
        props.put("buffer.memory", 10000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(topicName, Integer.toString(i),
                    Integer.toString(i)));

        }

        producer.close();


    }

}
