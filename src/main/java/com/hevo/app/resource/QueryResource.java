package com.hevo.app.resource;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.glassfish.jersey.server.ContainerRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;

@Api
@Path("/data")
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {

    private final QueryHandler handler;

    @Inject
    public QueryResource(QueryHandler handler) {
        this.handler = handler;
    }

    @GET
    @Timed
    @Path("/search")
    @ApiResponses({@ApiResponse(code = 404, message = "Query not found")})
    public QueryResponse query(@Context Request request) {
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
        queryParams.putAll(((ContainerRequest) request).getUriInfo().getQueryParameters());
        return handler.apiRequestQueryHandler(queryParams);
    }

    @GET
    @Timed
    @Path("/delete/{indexName}")
    @ApiResponses({@ApiResponse(code = 404, message = "Index not found")})
    public QueryResponse delete(@PathParam("indexName") String index) {
        return handler.requestHandler(index);
    }
}
