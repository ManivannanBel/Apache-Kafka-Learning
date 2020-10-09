package kafka.learning3;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ElasticsearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        //create kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_topic");

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            int recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequests = new BulkRequest();
            for(ConsumerRecord<String, String> record : records){
                String jsonString = record.value();

                //By passing id, we can make it idempotent (i.e, our system don't affected by duplicate messages)
                //ID generation strategy 1 (generic kafka id)
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //Use ID from Tweet JSON strategy 2
                try{
                    String id = extractTweeId(jsonString);

                    //Create index request for elastic search
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(jsonString, XContentType.JSON);
                    bulkRequests.add(indexRequest);

//                //Create index
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                logger.info(indexResponse.getId());
//                //Some delay of 10ms
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data : " + jsonString);
                }
            }
            if(recordCount > 0){
                BulkResponse bulkItemResponses = client.bulk(bulkRequests, RequestOptions.DEFAULT);
                logger.info("committing offsets....");
                consumer.commitSync();
                logger.info("Offsets have been committed");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //client.close();
    }

    public  static JsonParser jsonParser = new JsonParser();
    public static String extractTweeId(String jsonString){
        String id = jsonParser.parse(jsonString)
                        .getAsJsonObject()
                        .get("id_str")
                        .getAsString();
        return id;
    }

    public static KafkaConsumer createKafkaConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-twitter-application";

        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest, latest, none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  //We should commit manually
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");   //fetch at most 100 records per poll()

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static RestHighLevelClient createClient(){
        final String hostname = "kafka-learning-4970476006.eu-central-1.bonsaisearch.net";
        final String username = "o3fk8hv4yv";
        final String password = "ec3pvgevd8";

        //Dont do in case of local elastic search
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

                RestHighLevelClient client = new RestHighLevelClient(builder);
                return client;
    }
}
