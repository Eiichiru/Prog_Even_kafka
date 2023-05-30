package fr.isri.kafka;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class producer {

    public static Random rand = new Random();

    public static void main(String[] args) throws InterruptedException {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        while(true) {

            System.out.println("envoyé");
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("temp_Celsius", String.valueOf(rand.nextInt(80))); // Génère un nombre entre 0 (inclus) et 11 (exclus), puis ajoute 20

            // send data - asynchronous
            producer.send(producerRecord);

            // flush data - synchronous
            producer.flush();

            Thread.sleep(3000);
        }

        // flush and close producer
        //producer.close();

    }
}
