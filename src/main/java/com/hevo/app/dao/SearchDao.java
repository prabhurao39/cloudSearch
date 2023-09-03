package com.hevo.app.dao;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.hevo.app.client.ElasticSearchFactory;
import com.hevo.app.exceptions.ElasticException;
import com.hevo.app.model.CloudDocument;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class SearchDao<T> {
    private static final ElasticsearchClient esClient = ElasticSearchFactory.getEsClient();

    /**
     * search ES for the given query
     *
     * @param field      column to match
     * @param searchText text to be searched
     * @return a list of matching documents from ES
     * @throws ElasticException throws an IO exception
     */
    public List<T> searchDocumentByField(String indexName, String field, String searchText,
                                         Class<? extends CloudDocument> target)
            throws ElasticException {

        SearchResponse<T> response;
        try {
            // search an ES index matching the given field & value
            response = esClient.search(s -> s
                            .index(indexName)
                            .query(q -> q.match(t -> t
                                            .field(field)
                                            .query(searchText)
                                    )
                            ),
                    (Type) Class.forName(target.getName())
            );
        } catch (IOException | ClassNotFoundException ex) {
            throw new ElasticException("Failed to search documents " + ex.getMessage());
        }
        return transformSearchResponse(response);
    }

    /**
     * reads the response from ES
     *
     * @param searchResponse search response for matching query
     * @return list of documents
     */
    public List<T> transformSearchResponse(SearchResponse<T> searchResponse) {
        // read the hits from the response
        List<Hit<T>> hits = searchResponse.hits().hits();
        List<T> response = new ArrayList<>();
        for (Hit<T> hit : hits) {
            T t = hit.source();
            response.add(t);
        }
        return response;
    }

}
