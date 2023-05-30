package fr.isri.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class traducteur

{
    private static final Logger log = LoggerFactory.getLogger(producer.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka traducteur");

        String bootstrapServersIN = "127.0.0.1:9092";
        String groupIdIN = "my-fourth-application";
        String topicIN = "temp_Celcius";

        // create consumer configs
        Properties propertiesIN = new Properties();
        propertiesIN.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersIN);
        propertiesIN.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesIN.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesIN.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdIN);
        propertiesIN.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create producer configs
        Properties propertiesOUT = new Properties();
        propertiesOUT.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propertiesOUT.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesOUT.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

         // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesIN);

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesOUT);


        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topicIN));
           // poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){

                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("temp_Fahrenheit", String.valueOf(Integer.valueOf(record.value()) * (9/5) + 32));
                // send data - asynchronous
                producer.send(producerRecord);

                // flush data - synchronous
                producer.flush();

                System.out.println("traduit et envoyé");

                Thread.sleep(300);
            }
        }
    }
}
