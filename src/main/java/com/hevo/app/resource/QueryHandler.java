package com.hevo.app.resource;

import com.google.inject.Inject;
import com.hevo.app.dao.CrudDao;
import com.hevo.app.dao.SearchDao;
import com.hevo.app.exceptions.ElasticException;
import com.hevo.app.model.Product;
import com.hevo.app.model.Word;
import com.hevo.app.model.CloudDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

import static com.hevo.app.constants.CloudSearchConstants.DOCS_FOUND;
import static com.hevo.app.constants.CloudSearchConstants.DOCS_NOT_FOUND;
import static com.hevo.app.constants.CloudSearchConstants.EXCEPTION_OCCURRED;
import static com.hevo.app.constants.CloudSearchConstants.FAILED_TO_DELETED_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.FILE_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.PRODUCT_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.PRODUCT_NAME;
import static com.hevo.app.constants.CloudSearchConstants.Q;
import static com.hevo.app.constants.CloudSearchConstants.SUCCESSFULLY_DELETED_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.WORDS;

/**
 * class to handle the incoming request
 */
public class QueryHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);

    private final CrudDao crudDao;
    private final SearchDao searchDao;

    @Inject
    public QueryHandler(CrudDao crudDao, SearchDao searchDao) {
        this.crudDao = crudDao;
        this.searchDao = searchDao;
    }

    /**
     * prepares request data for the query
     *
     * @param queryParams query params from the request
     * @return query response
     */
    public QueryResponse<T> apiRequestQueryHandler(MultivaluedMap<String, String> queryParams) {
        String key = queryParams.containsKey(Q) ? Q : PRODUCT_NAME;
        String value = queryParams.getFirst(key);
        String field = queryParams.containsKey(Q) ? WORDS : PRODUCT_NAME;
        String indexName = field.equals(WORDS) ? FILE_INDEX : PRODUCT_INDEX;
        Class<? extends CloudDocument> targetDoc = queryParams.containsKey(Q) ? Word.class : Product.class;

        return apiRequestQueryHandler(field, value, targetDoc, indexName);
    }

    /**
     * invokes call to search a document
     *
     * @param field      field to match for given searchText
     * @param searchText value to search
     * @param target     document type to search
     * @param indexName  index name to search
     * @return returns ES response
     */
    public QueryResponse<T> apiRequestQueryHandler(String field, String searchText, Class<? extends CloudDocument> target, String indexName) {
        List<T> response;
        try {
            response = searchDao.searchDocumentByField(indexName, field, searchText, target);
            QueryResponse<T> queryResponse = new QueryResponse<>();
            queryResponse.setMessage(response.isEmpty() ? DOCS_NOT_FOUND : DOCS_FOUND);
            queryResponse.setList(response);
            return queryResponse;
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info(String.format(EXCEPTION_OCCURRED + " %s", e.getMessage()));
            return new QueryResponse<T>().setMessage(EXCEPTION_OCCURRED);
        }
    }


    /**
     * invokes call to delete a document
     *
     * @param index index to delete
     * @return query response
     */
    public QueryResponse<T> requestHandler(String index) {
        try {
            crudDao.deleteIndex(index);
            return new QueryResponse<T>().setMessage(String.format(SUCCESSFULLY_DELETED_INDEX + " %s ", index));
        } catch (ElasticException e) {
            LOGGER.info(e.getMessage());
        }
        return new QueryResponse<T>().setMessage(String.format(FAILED_TO_DELETED_INDEX + " %s ", index));
    }

}
