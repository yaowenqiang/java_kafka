package com.jacky.kafka;


import jdk.javadoc.internal.doclets.formats.html.IndexRedirectWriter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticSearchConsumer {


    public static RestClient createClient() {
        //https://:gtf0fkw9eo@
        String hostname = "kafka-demo-729430487.us-east-1.bonsaisearch.net:443";
        String username = "1v6zobwo9i";
        String password = "gtf0fkw9eo";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient client = new RestClient(builder);
        return client;

    }
    public static void main(String[] args) {
        RestClient = createClient();
        SearchResponse<Product> search = client.search(s -> s
        .index("products")
        .query(q -> q
        .term(t -> t
        .field("name")
        .value(v-> v.stringValue("bicycle")))));
    }
}
