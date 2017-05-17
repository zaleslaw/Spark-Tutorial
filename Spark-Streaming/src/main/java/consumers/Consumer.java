package consumers;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("session.timeout", 10000);
        props.put("group.id", "test");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("messages"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(e ->
                    System.out.println("Offset " + e.offset() + " key= " + e.key() + " value= " + e.value()));
        }

    }
}
