package doubloon;

import org.apache.kafka.clients.producer.*;

public class Writer implements Producer{

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Writer(String servers, String topic) {
        this.producer = new KafkaProducer<String, String>(Producer.createConfig(servers));
        this.topic = topic;
    }
    @Override
    public void produce(String message) {
        ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, message);
        producer.send(pr);
    }
}
