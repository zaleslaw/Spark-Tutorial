package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MultithreadingProducer {
    public static void main(String[] args) {
        String topicName = "messages";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", 65536);
        props.put("buffer.memory", 10000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);


        AtomicInteger counter = new AtomicInteger(0);

        ScheduledExecutorService service = Executors.newScheduledThreadPool(20);

        Runnable lambda = () -> {
            String result = String.valueOf(counter.getAndIncrement());
            try {
                producer.send(new ProducerRecord<>(
                        topicName,
                        String.valueOf(ThreadLocalRandom.current().nextInt(10)),
                        result));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        };

        ScheduledFuture scheduledTask = service
                .scheduleAtFixedRate(lambda, 0, 1, TimeUnit.MILLISECONDS);

        service.schedule((Runnable) () -> scheduledTask
                .cancel(true), 6000, SECONDS);
        while (!scheduledTask.isCancelled()) {

        }
        try {
            service.awaitTermination(1, SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        service.shutdown();
    }
}
