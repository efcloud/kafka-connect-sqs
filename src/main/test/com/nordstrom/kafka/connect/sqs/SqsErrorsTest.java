package com.nordstrom.kafka.connect.sqs;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SqsException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqsErrorsTest {

    static AwsServiceException serviceError(int status, String code) {
        return SqsException.builder()
                .statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
                .build();
    }

    @Test
    void clientSideFailuresAreRetriable() {
        assertTrue(SqsErrors.isRetriable(SdkClientException.create("Unable to execute HTTP request")));
        assertTrue(SqsErrors.isRetriable(ApiCallTimeoutException.create(1000)));
    }

    @Test
    void serverErrorsAndThrottlingAreRetriable() {
        assertTrue(SqsErrors.isRetriable(serviceError(500, "InternalError")));
        assertTrue(SqsErrors.isRetriable(serviceError(503, "ServiceUnavailable")));
        assertTrue(SqsErrors.isRetriable(serviceError(400, "ThrottlingException")));
        assertTrue(SqsErrors.isRetriable(serviceError(429, "TooManyRequests")));
    }

    @Test
    void permanentFailuresAreNotRetriable() {
        assertFalse(SqsErrors.isRetriable(serviceError(403, "AccessDenied")));
        assertFalse(SqsErrors.isRetriable(QueueDoesNotExistException.builder().statusCode(400).build()));
        assertFalse(SqsErrors.isRetriable(new IllegalArgumentException("bad url")));
    }
}
