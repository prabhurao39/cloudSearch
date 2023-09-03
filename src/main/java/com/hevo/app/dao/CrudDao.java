package com.hevo.app.dao;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.hevo.app.client.ElasticSearchFactory;
import com.hevo.app.exceptions.ElasticException;
import com.hevo.app.model.Product;
import com.hevo.app.model.Word;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hevo.app.constants.CloudSearchConstants.FAILED_TO_CREATE_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.FILE_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.PRODUCT_INDEX;

public class CrudDao {

    private static final ElasticsearchClient esClient = ElasticSearchFactory.getEsClient();
    private static final List<String> indices = new ArrayList<>();

    /**
     * creates an index in elasticsearch
     *
     * @throws ElasticException throws custom exception
     */
    public void createIndices() throws ElasticException {
        try {
            for (String indexName : getIndices()) {
                BooleanResponse res = esClient.indices().exists(ExistsRequest.of(e -> e.index(indexName)));
                if (!res.value()) {
                    esClient.indices().create(c -> c.index(indexName));
                    updateIndexSettings(indexName);
                }
            }
        } catch (IOException ex) {
            throw new ElasticException(FAILED_TO_CREATE_INDEX + ex.getMessage());
        }
    }

    /**
     * updates the index settings of the elasticsearch
     * @param indexName
     * @throws IOException
     */
    private void updateIndexSettings(String indexName) throws IOException {
        String jsonString = "{"
                + "\"index\": {"
                + "\"number_of_replicas\": \"0\","
                + "\"refresh_interval\": \"10s\""
                + "}"
                + "}";
        RestClient restClient = ElasticSearchFactory.getRestClient();
        Request request = new Request("PUT", "/" + indexName + "/_settings");
        request.setJsonEntity(jsonString);
        restClient.performRequest(request);
    }

    /**
     * creates a document in elasticsearch
     *
     * @param product document to insert
     * @throws ElasticException throws custom exception
     */
    public void ingestDocumentsForProduct(Product product) throws ElasticException {
        try {
            esClient.index(i -> i
                    .index(PRODUCT_INDEX)
                    .id(product.getFilePath())
                    .document(product)
            );
        } catch (IOException ex) {
            throw new ElasticException(" Failed to ingest document : " + ex.getMessage());
        }
    }

    /**
     * creates a document in elasticsearch
     *
     * @param words document to ingest
     * @throws ElasticException throws an IO exception
     */
    public void indexDocumentsForFile(Word words) throws ElasticException {
        try {
            esClient.index(i -> i
                    .index(FILE_INDEX)
                    .id(words.getFilePath())
                    .document(words)
            );
        } catch (IOException ex) {
            throw new ElasticException(" Failed to ingest document : " + ex.getMessage());
        }
    }

    /**
     * @return list of indices to create
     */
    private List<String> getIndices() {
        indices.add(FILE_INDEX);
        indices.add(PRODUCT_INDEX);
        return indices;
    }

    /**
     * deletes an documents in elasticsearch
     * @param indexName indexName
     * @throws ElasticException throws a custom exception
     */
    public void deleteIndex(String indexName) throws ElasticException {
        try {
            esClient.indices().delete(c -> c.index(indexName));
        } catch (IOException ex) {
            throw new ElasticException(" Failed to delete document : " + ex.getMessage());
        }
    }

    /**
     * deletes a document by Id
     * @param indexName indexName
     * @param id id
     * @throws Exception throws exception
     */
    public void deleteDocById(String indexName, String id) throws Exception {
        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(id));
        esClient.delete(request);
    }

}
