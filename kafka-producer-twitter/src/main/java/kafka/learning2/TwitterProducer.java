package apache.kafka.learning2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    final String bootstrapServer = "127.0.0.1:9092";
    final String topic = "twitter_topic";

    final String consumerKey = "kZT3T8RacT4g0knOIHJhj6kqa";
    final String consumerSecret = "pIdbx5lp5XFEd4Petjj1Yp6WrmgkwyiOg4m8Ejc0JNcicsFkdG";
    final String token = "577124705-5xHAgCoEgLEkLrZTQej0gn5RHKSrjEUGjdekWLRZ";
    final String secret = "L8fyZXoGUmQHpNst8ErYlhwtkgXcUMdt4VC3hgouuKPzr";

    public TwitterProducer(){

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //Create twitter client
        Client client = createTwitterClient(msgQueue);
        //Estabish a connection
        client.connect();

        //Create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //Add shutdown hook for exiting application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing the Application");
            logger.info("Stopping the twitter client...");
            client.stop();
            logger.info("Closing the producer");
            producer.close();
            logger.info("Application exited!");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                client.stop();
            }
            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null){
                            logger.error("Something happened ", exception);
                        }
                    }
                });
            }
        }

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("usa", "politics", "corona", "game");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka safe producer properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Kafka 2.13 > kafka 0.1, so use 5 otherwise 1

        //High throughput(At the expense of latency and CPU usage)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //delay by 20ms
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32KB

        //Create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return  producer;
    }

}
