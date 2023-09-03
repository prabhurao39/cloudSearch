package com.hevo.app.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import static com.hevo.app.constants.CloudSearchConstants.SERVER_URL;

public class ElasticSearchFactory {

    private ElasticSearchFactory() {}

    /**
     * method to generate ES rest client
     * @return returns elastic search client
     */
    public static ElasticsearchClient getEsClient() {
        // URL and API key


        RestClient restClient = getRestClient();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create & return the API client
        return new ElasticsearchClient(transport);
    }

    public static RestClient getRestClient() {
        // Create the low-level client
        RestClient restClient = RestClient
                .builder(HttpHost.create(SERVER_URL))
                .build();
        return restClient;
    }


}
