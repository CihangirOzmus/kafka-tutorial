package com.cigi.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        String hostname = System.getenv("ES_HOSTNAME");
        String username = System.getenv("ES_USERNAME");
        String password = System.getenv("ES_PASSWORD");

        //dont do for local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String jsonString = "{\"foo\":\"bar\"}";
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        request.source(jsonString, XContentType.JSON);

        BulkRequest bulkRequest = new BulkRequest();
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar")
                .endObject();

        IndexRequest indexRequest = new IndexRequest("twitter");
        indexRequest.source(builder);
        bulkRequest.add(indexRequest);

        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse item : responses.getItems()) {
            logger.info(item.getId());
        }

        client.close();
    }
}
