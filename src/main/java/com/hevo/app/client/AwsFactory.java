package com.hevo.app.client;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hevo.app.ManagedAwsClient;
import io.dropwizard.setup.Environment;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class AwsFactory {

    @JsonProperty
    private String awsAccessKeyId;

    @JsonProperty
    private String awsSecretKey;

    @JsonProperty
    private String awsRegion;

    /**
     * builds a S3 client based
     * @param environment env
     * @return amazon s3 object
     */
    public AmazonS3 buildAmazonS3Client(Environment environment) {
        AmazonS3 s3;
        if (isEmpty(awsAccessKeyId) || isEmpty(awsSecretKey)) {
            s3 = AmazonS3ClientBuilder.standard().withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                    .withRegion(awsRegion)
                    .build();
        } else {
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
            s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion(awsRegion)
                    .build();
        }
        environment.lifecycle().manage(new ManagedAwsClient((AmazonWebServiceClient) s3));
        return s3;
    }

    /**
     * builds a sqs client
     * @param environment env
     * @return amazon sqs object
     */
    public AmazonSQS buildAmazonSQSClient(Environment environment) {

        AmazonSQS sqs;

        if (isEmpty(awsAccessKeyId) || isEmpty(awsSecretKey)) {
            sqs = AmazonSQSClientBuilder.standard().withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                    .withRegion(awsRegion)
                    .build();
        } else {
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
            sqs = AmazonSQSClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion(awsRegion)
                    .build();
        }
        environment.lifecycle().manage(new ManagedAwsClient((AmazonWebServiceClient) sqs));
        return sqs;
    }
}
