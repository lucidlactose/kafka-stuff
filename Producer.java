import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String clientId = "my-producer";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("cliend.id", clientId);

        KafkaProducer<Number, String> producer = new KafkaProducer<>(props);
        int numOfRecords = 100;
        String topic = "numbers";

        try {
            for (int i = 0; i < numOfRecords; ++i) {
                String message = String.format("producer %s send message %s at %s", clientId, i, new Date());
                System.out.println(message);
                producer.send(new ProducerRecord<>(topic, i, message));
//                Thread.sleep(300);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

        producer.close();
    }
}
