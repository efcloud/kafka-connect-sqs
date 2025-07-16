package com.nordstrom.kafka.connect.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import org.apache.kafka.common.Configurable;

import java.util.Map;

public class AWSUserCredentialsProvider implements AwsCredentialsProvider, Configurable {
    private static final String AWS_ACCESS_KEY_ID = "accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "secretKey";

    private String awsAccessKeyId;
    private String awsSecretKey;

    private volatile AwsCredentials cachedCredentials;

    @Override
    public AwsCredentials resolveCredentials() {
        if (awsAccessKeyId == null || awsSecretKey == null) {
            throw new IllegalStateException("AWSUserCredentialsProvider has not been configured – call configure() " +
                    "first. AWS Access Key ID and Secret Key must be set. ");
        }

        // cache to avoid needless object churn
        if (cachedCredentials == null) {
            cachedCredentials = AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey);
        }
        return cachedCredentials;
    }

    @Override
    public void configure(Map<String, ?> map) {
        awsAccessKeyId = getRequiredField(map, AWS_ACCESS_KEY_ID);
        awsSecretKey = getRequiredField(map, AWS_SECRET_ACCESS_KEY);
    }


    private String getRequiredField(final Map<String, ?> map, final String fieldName) {
        final Object field = map.get(fieldName);
        verifyNotNull(field, fieldName);
        final String fieldValue = field.toString();
        verifyNotNullOrEmpty(fieldValue, fieldName);

        return fieldValue;
    }

    private boolean isNotNull(final Object field) {
        return null != field;
    }

    private boolean isNotNullOrEmpty(final String field) {
        return null != field && !field.isEmpty();
    }

    private void verifyNotNull(final Object field, final String fieldName) {
        if (!isNotNull(field)) {
            throw new IllegalArgumentException(String.format("The field '%1s' should not be null", fieldName));
        }
    }

    private void verifyNotNullOrEmpty(final String field, final String fieldName) {
        if (!isNotNullOrEmpty(field)) {
            throw new IllegalArgumentException(String.format("The field '%1s' should not be null or empty", fieldName));
        }
    }
}
