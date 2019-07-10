package doubloon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

public class Validator implements Producer{

    private final KafkaProducer<String, String> producer;
    private final String goodTopic;
    private final String badTopic;
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    public Validator(String servers, String goodTopic, String badTopic) {
        this.producer = new KafkaProducer<String, String>(Producer.createConfig(servers));
        this.goodTopic = goodTopic;
        this.badTopic = badTopic;
    }

    public String validate(JsonNode root, String path){
        if(!root.has(path)){
            return path + " is missing";
        }
        JsonNode node = root.path(path);
        if(node.isMissingNode()){
            return path + " is missing ";
        }
        return "";
    }

    @Override
    public void produce(String message) {

        ProducerRecord<String, String> pr = null;
        JsonNode root = null;
        String messageValue = "";
        String topic = "";

        try {

            root = MAPPER.readTree(message);
            String fields[] = {"event", "customer", "currency", "timestamp"};
            String error = "";


            for (String currentField : fields) {
                error = error.concat(validate(root, currentField));
            }
            if (error.length() > 0) {
                messageValue = String.format("{\"error\": %s}", error);
                topic = this.badTopic;
            } else {
                messageValue = String.format("{\"error\": %s}", MAPPER.writeValueAsString(root));
                topic = this.goodTopic;
            }

        } catch (IOException e) {

            e.printStackTrace();
            messageValue = String.format("{\"error\": %s}", e.getClass().getSimpleName() + ": " + e.getMessage());
            topic = this.badTopic;

        } finally {

            pr = new ProducerRecord<String, String>(topic, messageValue);
            if (pr != null){
                producer.send(pr);
            }
        }


    }
}
