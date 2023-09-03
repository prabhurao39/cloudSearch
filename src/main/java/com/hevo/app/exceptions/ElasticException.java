package com.hevo.app.exceptions;

import com.hevo.app.constants.CloudSearchConstants;

public class ElasticException extends Exception {
    public ElasticException(String message) {
        super(message);
    }
}
