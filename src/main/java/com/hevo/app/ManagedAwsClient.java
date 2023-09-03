package com.hevo.app;

import com.amazonaws.AmazonWebServiceClient;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedAwsClient implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedAwsClient.class);

    private final AmazonWebServiceClient awsClient;

    public ManagedAwsClient(AmazonWebServiceClient awsClient) {
        if (awsClient == null) {
            throw new IllegalArgumentException("Aws client cannot be null");
        }
        this.awsClient = awsClient;
    }

    @Override
    public void start() {
        LOGGER.info("Starting the AWS Client, {}", awsClient.getClass());
    }

    @Override
    public void stop() {
        LOGGER.info("Shutting down aws client, {}", awsClient.getClass());
        awsClient.shutdown();
    }

}
