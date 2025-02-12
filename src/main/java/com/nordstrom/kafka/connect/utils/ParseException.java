package com.nordstrom.kafka.connect.utils;

import org.apache.kafka.common.errors.RetriableException;

public class ParseException extends RetriableException {
    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
