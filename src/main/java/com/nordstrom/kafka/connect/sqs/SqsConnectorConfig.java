package com.nordstrom.kafka.connect.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

abstract public class SqsConnectorConfig extends AbstractConfig {
    private final String queueUrl;
    private final String topics;
    private final String region;
    private final String endpointUrl;
    private final Boolean transformToJson;
    private final String timestampFormat;

    public SqsConnectorConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals);
        queueUrl = getString(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue());
        topics = getString(SqsConnectorConfigKeys.TOPICS.getValue());
        region = getString(SqsConnectorConfigKeys.SQS_REGION.getValue());
        endpointUrl = getString(SqsConnectorConfigKeys.SQS_ENDPOINT_URL.getValue());
        transformToJson = getBoolean(SqsConnectorConfigKeys.VALUE_TRANSFORM_TO_JSON.getValue());
        timestampFormat = getString(SqsConnectorConfigKeys.VALUE_TRANSFORM_TIMESTAMP_FORMAT.getValue());
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public String getTopics() {
        return topics;
    }

    public String getRegion()  {
        return region;
    }

    public String getEndpointUrl()  {
        return endpointUrl;
    }

    public Boolean getTransformToJson() { return transformToJson; }

    public String getTimestampFormat() { return timestampFormat; }

    protected static class CredentialsProviderValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object provider) {
            if (provider instanceof Class
                    && AWSCredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
                return;
            }
            throw new ConfigException(
                    name,
                    provider,
                    "Class must extend: " + AWSCredentialsProvider.class
            );
        }

        @Override
        public String toString() {
            return "Any class implementing: " + AWSCredentialsProvider.class;
        }
    }
}
