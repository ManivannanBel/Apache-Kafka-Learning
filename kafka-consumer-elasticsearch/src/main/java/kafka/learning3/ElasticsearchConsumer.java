package kafka.learning3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticsearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String jsonString = "{\"foo\" : \"bar\"}";

        CreateIndexRequest indexRequest = new CreateIndexRequest("twitter");
        indexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 2)
        );
        indexRequest.mapping("tweets", jsonString, XContentType.JSON);
        CreateIndexResponse response = client.indices().create(indexRequest, RequestOptions.DEFAULT);
        String id = response.index();
        logger.info(id);

        client.close();
    }

    public static RestHighLevelClient createClient(){
        final String hostname = "https://o3fk8hv4yv:ec3pvgevd8@kafka-learning-4970476006.eu-central-1.bonsaisearch.net:443";
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
