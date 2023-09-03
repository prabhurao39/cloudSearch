package com.hevo.app;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.hevo.app.resource.QueryResource;
import com.hevo.app.dao.CrudDao;
import com.hevo.app.healthcheck.AppHealthCheck;
import com.hevo.app.s3service.S3FileServiceImpl;
import com.hevo.app.s3service.S3FileProcessor;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudSearchApp extends Application<CloudSearchConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudSearchApp.class);
    public static void main(String[] args)
            throws Exception {
        new CloudSearchApp().run(args);
    }

    @Override
    public void initialize(Bootstrap<CloudSearchConfig> b) {}

    @Override
    public void run(CloudSearchConfig configuration, Environment environment) {
        final Injector injector = Guice.createInjector(getModule(configuration, environment));
        final AmazonS3 s3Client = configuration.getAwsFactory().buildAmazonS3Client(environment);
        final AmazonSQS sqsClient = configuration.getAwsFactory().buildAmazonSQSClient(environment);

        //resources
        environment.jersey().register(injector.getInstance(QueryResource.class));
        //health check
        environment.healthChecks().register("Cloudsearch", new AppHealthCheck());
        CrudDao crudDao = new CrudDao();
        S3FileProcessor s3FileProcessor = new S3FileProcessor(s3Client, configuration, sqsClient, crudDao);
        S3FileServiceImpl fileService = new S3FileServiceImpl(s3FileProcessor, crudDao);
        environment.lifecycle().manage(fileService);
        LOGGER.info("Cloud Search App started running.");
    }

    protected Module getModule(CloudSearchConfig configuration, Environment environment) {
        return new CloudSearchModule(configuration, environment);
    }

}
