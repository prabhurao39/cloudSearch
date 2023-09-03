package com.hevo.app.s3service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.hevo.app.CloudSearchConfig;
import com.hevo.app.constants.DocType;
import com.hevo.app.dao.CrudDao;
import com.hevo.app.exceptions.ElasticException;
import com.hevo.app.exceptions.S3Exception;
import com.hevo.app.model.CloudDocument;
import com.hevo.app.model.Product;
import com.hevo.app.model.Word;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.hevo.app.constants.CloudSearchConstants.BACK_SLASH;
import static com.hevo.app.constants.CloudSearchConstants.BUCKET;
import static com.hevo.app.constants.CloudSearchConstants.DELETE;
import static com.hevo.app.constants.CloudSearchConstants.DELETED_FILE_IN_ES;
import static com.hevo.app.constants.CloudSearchConstants.EVENT_NAME;
import static com.hevo.app.constants.CloudSearchConstants.FAILED_TO_DELETED_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.FAILED_TO_READ_FROM_S_3;
import static com.hevo.app.constants.CloudSearchConstants.FAILED_WHILE_PROCESSING_JSON;
import static com.hevo.app.constants.CloudSearchConstants.FILE_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.INGESTED_FILE_IN_ES;
import static com.hevo.app.constants.CloudSearchConstants.KEY;
import static com.hevo.app.constants.CloudSearchConstants.NAME;
import static com.hevo.app.constants.CloudSearchConstants.OBJECT;
import static com.hevo.app.constants.CloudSearchConstants.PRODUCT_INDEX;
import static com.hevo.app.constants.CloudSearchConstants.RECORDS;
import static com.hevo.app.constants.CloudSearchConstants.REGEX;
import static com.hevo.app.constants.CloudSearchConstants.S_3;

public class S3FileProcessor implements CloudDocument {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileProcessor.class);


    private final AmazonS3 s3;
    private final AmazonSQS sqs;
    private final CloudSearchConfig config;
    private final CrudDao crudDao;

    @Inject
    public S3FileProcessor(AmazonS3 s3, CloudSearchConfig config, AmazonSQS sqs, CrudDao crudDao) {
        this.s3 = s3;
        this.sqs = sqs;
        this.config = config;
        this.crudDao = crudDao;
    }

    /**
     * reads file from s3 for given bucket name
     *
     * @param s3Client   aws s3 client
     * @param bucketName bucket name to look up
     * @param path       file path to read
     * @throws ElasticException throws s3 exception
     */
    private void readFromS3(AmazonS3 s3Client, String bucketName, String path) throws ElasticException {
        try {
            fetchJsonFile(s3Client, bucketName, path);
            streamTxtFile(s3Client, bucketName, path);
            LOGGER.info(INGESTED_FILE_IN_ES + path);
        } catch (ElasticException ex) {
            throw new ElasticException(ex.getMessage());
        } catch (JsonProcessingException e) {
            LOGGER.info(FAILED_WHILE_PROCESSING_JSON + e.getMessage());
        } catch (IOException e) {
            LOGGER.info(FAILED_TO_READ_FROM_S_3 + e.getMessage());
        }
    }

    /**
     * Fetches the complete Json file as string & processes it
     *
     * @param s3Client s3 client
     * @param bucketName name of the bucket
     * @param path name of the file with its path
     * @throws JsonProcessingException throws json exception
     * @throws ElasticException throws custom exception
     */
    private void fetchJsonFile(AmazonS3 s3Client, String bucketName, String path) throws JsonProcessingException,
            ElasticException {
        String s3ObjectString;
        if (path.contains(DocType.JSON.getLowercase())) {
//             reads file from s3 folders
            s3ObjectString = s3Client.getObjectAsString(bucketName, path);
            String httpUrl  = ((AmazonS3Client) s3Client).getResourceUrl(bucketName, path);
            extractAndIngestProduct(bucketName, path, s3ObjectString, httpUrl);
        }
    }

    /**
     * Streams the s3 file line by line
     *
     * @param s3Client s3 client
     * @param bucketName name of the bucket
     * @param path name of the file with its path
     * @throws IOException throws IO exception
     * @throws ElasticException throws custom exception
     */
    private void streamTxtFile(AmazonS3 s3Client, String bucketName, String path) throws IOException, ElasticException {
        if (path.contains(DocType.TXT.getLowercase())) {
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, path));
            InputStream objectData = s3Object.getObjectContent();
            String httpUrl  = ((AmazonS3Client) s3Client).getResourceUrl(bucketName, path);
            // read the streamline by line
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            String line;
            while ((line = reader.readLine()) != null) {
                // Process each line as needed.
                splitAndIngestWord(bucketName, path, line, httpUrl);
            }

            // TODO : write lambda connecting SQS to chunk data and push to another SQS
            // TODO : use SQS to store chunked message in case of very large files

        }
    }

    /**
     * read & ingest the file content to ES
     * @param line content of the file
     * @param bucketName name of the bucket
     * @param path name of the file with its path
     * @throws ElasticException throws custom exception
     */
    private void splitAndIngestWord(String bucketName, String path, String line, String httpUrl) throws ElasticException {
        // for text, file index & word model is used
        Word words = new Word();
        String[] letters = line.split(REGEX);
        List<String> list = Arrays.asList(letters);
        words.setWords(list);
        words.setHttpUrl(httpUrl);
        words.setFilePath(bucketName + BACK_SLASH + path);
        crudDao.indexDocumentsForFile(words);
    }

    /**
     * maps the s3 content to Product doc and ingests to ES
     * @param bucketName name of the bucket
     * @param path name of the file with its path
     * @param s3Object content of the file
     * @param httpUrl url of the file
     * @throws JsonProcessingException throws json exception
     * @throws ElasticException throws custom exception
     */
    private void extractAndIngestProduct(String bucketName, String path, String s3Object, String httpUrl)
            throws JsonProcessingException, ElasticException {
        // based on doc type : indices & model vary
        // for json, product index & product model is used
        ObjectMapper mapper = new ObjectMapper();
        Product product = mapper.readValue(s3Object, Product.class);
        product.setFilePath(bucketName + BACK_SLASH + path);
        product.setHttpUrl(httpUrl);
        crudDao.ingestDocumentsForProduct(product);
    }

    /**
     * fetch and process the sqs message
     *
     * @param s3Client          s3 client to read files
     * @param sqs               sqs client to events in s3
     * @param sqsListenQueueUrl sqs queue to poll events
     * @param message           message from sqs event
     * @throws S3Exception throws custom exception
     */
    private void processMessage(AmazonS3 s3Client, AmazonSQS sqs, String sqsListenQueueUrl, Message message)
            throws S3Exception {
        try {
            String body = message.getBody();
            // Initialize Jackson ObjectMapper
            ObjectMapper objectMapper = new ObjectMapper();

            // Parse the message body into JsonNode
            JsonNode jsonNode = objectMapper.readTree(body);
            if (jsonNode.get(RECORDS) != null) {
                JsonNode s3Node = jsonNode.get(RECORDS).get(0).get(S_3);
                JsonNode bucketNode = s3Node.get(BUCKET);
                JsonNode objectNode = s3Node.get(OBJECT);
                String keyPath = objectNode.get(KEY).asText();
                String bucketName = bucketNode.get(NAME).asText();

                String eventName = jsonNode.get(RECORDS).get(0).get(EVENT_NAME).asText();

                // read the file for the event notified to sqs
                if (eventName.contains(DELETE))
                    deleteDocumentsFromES(bucketName, keyPath);
                else
                    readFromS3(s3Client, bucketName, keyPath);
            }
            // delete message in sqs
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(sqsListenQueueUrl, messageReceiptHandle));

        } catch (JsonProcessingException ex) {
            throw new S3Exception(ex.getMessage());
        } catch (Exception ex) {
            LOGGER.info(String.format(FAILED_TO_READ_FROM_S_3 + " %s", ex.getMessage()));
        }
    }

    /**
     * invokes delete call to ES
     * @param bucketName bucket to delete
     * @param keyPath file to delete
     */
    private void deleteDocumentsFromES(String bucketName, String keyPath) {
        String id = bucketName + BACK_SLASH + keyPath;
        String indexName = bucketName.contains(PRODUCT_INDEX)
                ? PRODUCT_INDEX : FILE_INDEX;
        try {
            crudDao.deleteDocById(indexName, id);
            LOGGER.info(DELETED_FILE_IN_ES + keyPath);
        } catch (Exception e) {
            LOGGER.info(String.format(FAILED_TO_DELETED_INDEX + "  %s", e.getMessage()));
        }
    }

    public void processInput() {
        readFromSqs(s3, sqs, config);
    }

    /**
     * polls SQS every 10 seconds
     *
     * @param s3     s3 client
     * @param sqs    sqs client
     * @param config application config
     */
    private void readFromSqs(AmazonS3 s3, AmazonSQS sqs, CloudSearchConfig config) {
        try {
            String sqsListenQueueUrl = config.getSqsEventNotifierName();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsListenQueueUrl)
                    .withMessageAttributeNames(new HashSet<>())
                    .withMaxNumberOfMessages(10).withWaitTimeSeconds(10);

            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            messages.forEach(msg -> {
                try {
                    processMessage(s3, sqs, sqsListenQueueUrl, msg);
                } catch (S3Exception ex) {
                    LOGGER.info(String.format(FAILED_TO_READ_FROM_S_3 + " %s", ex.getMessage()));
                }
            });

        } catch (Exception ex) {
            LOGGER.info(ex.getMessage());
        }
    }

}
