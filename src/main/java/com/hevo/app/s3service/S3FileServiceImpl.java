package com.hevo.app.s3service;

import com.hevo.app.constants.CloudSearchConstants;
import com.hevo.app.dao.CrudDao;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class to poll the sqs for s3 events
 */
public class S3FileServiceImpl extends FileService implements Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileServiceImpl.class);

    private Thread pollingThread;
    private final S3FileProcessor s3FileProcessor;
    private final CrudDao crudDao;

    public S3FileServiceImpl(S3FileProcessor s3FileProcessor, CrudDao crudDao) {
        this.s3FileProcessor = s3FileProcessor;
        this.crudDao = crudDao;
    }

    @Override
    public void start() {
        pollingThread = new Thread() {
            @Override
            public void run() {
                LOGGER.info("Start listening to s3");
                while (!isInterrupted()) {
                    try {
                        crudDao.createIndices();
                        pollSqsForEvents();
                    } catch (Exception e) {
                       LOGGER.info(e.getMessage());
                    }
                }
            }
        };
        pollingThread.start();
    }

    @Override
    public void stop() {
        pollingThread.interrupt();
    }

    public void pollSqsForEvents() {
        try {
            s3FileProcessor.processInput();
        } catch (Exception e) {
            LOGGER.info(CloudSearchConstants.EXCEPTION_OCCURRED + " " + e.getMessage());
        }
    }
}
