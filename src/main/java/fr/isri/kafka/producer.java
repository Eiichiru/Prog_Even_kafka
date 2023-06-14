package fr.isri.kafka;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
            producer.send(producerRecord, (metadata, exception) -> {
                //write on the prom file the celsius temp for node-exporter
                if (exception == null) {
                    try {
                        FileWriter fileWriter = new FileWriter("./textfile/temp_celsius.prom");
                        fileWriter.write("temp_Celsius{topic=\"temp_Celsius\"} " + producerRecord.value()+"\n" );
                        fileWriter.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    exception.printStackTrace();
                }
            });

            // flush data - synchronous
            producer.flush();

            Thread.sleep(5000);
        }

        // flush and close producer
        //producer.close();

    }
}
