package com.nordstrom.kafka.connect.sqs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.kafka.connect.About;
import com.nordstrom.kafka.connect.utils.KafkaJsonToStruct;
import com.nordstrom.kafka.connect.utils.ParseException;
import com.nordstrom.kafka.connect.utils.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
//import org.apache.kafka.connect.header.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.*;
import java.util.stream.Collectors;

public class SqsSourceConnectorTask extends SourceTask {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private SQSClient client;
    private SqsSourceConnectorConfig config;

    @Override
    public String version() {
        return About.CURRENT_VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("task.start");
        Guard.verifyNotNull(props, "Task properties");

        config = new SqsSourceConnectorConfig(props);
        client = new SQSClient(config);

        log.info("task.start.OK, sqs.queue.url={}, topics={}", config.getQueueUrl(), config.getTopics());
    }

    private String getPartitionKey(Message message) {
        String messageId = message.messageId();
        if (!config.getMessageAttributesEnabled()) {
            return messageId;
        }
        String messageAttributePartitionKey = config.getMessageAttributePartitionKey();
        if (StringUtils.isBlank(messageAttributePartitionKey)) {
            return messageId;
        }

        // search for the String message attribute with the same name as the configured partition key
        Map<String, MessageAttributeValue> attributes = message.messageAttributes();
        for (String attributeKey : attributes.keySet()) {
            if (!Objects.equals(attributeKey, messageAttributePartitionKey)) {
                continue;
            }
            MessageAttributeValue attrValue = attributes.get(attributeKey);
            if (!attrValue.dataType().equals("String")) {
                continue;
            }
            return attrValue.stringValue();
        }
        return messageId;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.debug(".poll:valid-state={}", isValidState());

        if (!isValidState()) {
            throw new IllegalStateException("Task is not properly initialized");
        }

        // Read messages from the queue.
        List<Message> messages = client.receive(
                config.getQueueUrl(),
                config.getMaxMessages(),
                config.getWaitTimeSeconds(),
                config.getMessageAttributesEnabled(),
                config.getMessageAttributesList());

        log.debug(".poll:url={}, max={}, wait={}, size={}",
                config.getQueueUrl(),
                config.getMaxMessages(),
                config.getWaitTimeSeconds(),
                messages.size());

        // Create a SourceRecord for each message in the queue.
        return messages.stream().map(message -> {
            Map<String, String> sourcePartition = Collections.singletonMap(
                    SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(),
                    config.getQueueUrl());

            Map<String, String> sourceOffset = new HashMap<>();
            sourceOffset.put(SqsConnectorConfigKeys.SQS_MESSAGE_ID.getValue(), message.messageId());
            sourceOffset.put(SqsConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue(), message.receiptHandle());

            log.trace(".poll:source-partition={}", sourcePartition);
            log.trace(".poll:source-offset={}", sourceOffset);

            final String body = message.body();
            final String key = getPartitionKey(message);
            final String topic = config.getTopics();

            final ConnectHeaders headers = new ConnectHeaders();
            if (config.getMessageAttributesEnabled()) {
                Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                // sqs api should return only the fields specified in the list
                for (String attributeKey : attributes.keySet()) {
                    MessageAttributeValue attrValue = attributes.get(attributeKey);
                    if (attrValue.dataType().equals("String")) {
                        SchemaAndValue schemaAndValue = new SchemaAndValue(
                                Schema.STRING_SCHEMA,
                                attrValue.stringValue());
                        headers.add(attributeKey, schemaAndValue);
                    }
                }
            }

            if (config.getTransformToJson()) {
                return parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);
            } else {
                return new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        null,
                        Schema.STRING_SCHEMA,
                        key,
                        Schema.STRING_SCHEMA,
                        body,
                        null,
                        headers);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        Guard.verifyNotNull(record, "record");
        final String receipt = record.sourceOffset()
                .get(SqsConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue())
                .toString();
        log.debug(".commit-record:url={}, receipt-handle={}", config.getQueueUrl(), receipt);
        client.delete(config.getQueueUrl(), receipt);
        super.commitRecord(record);
    }

    @Override
    public void stop() {
        log.info("task.stop:OK");
    }

    private boolean isValidState() {
        return null != config && null != client;
    }

    static SourceRecord parseJSON(String body, Map<String, String> sourcePartition,
                                  Map<String, String> sourceOffset, String topic, String key, ConnectHeaders headers) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap;
        try {
            jsonMap = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new ParseException("Failed to parse JSON from SQS", e);
        }

        SchemaBuilder schemaBuilder = KafkaJsonToStruct.buildSchema(jsonMap);
        Schema schema = schemaBuilder.build();
        Struct struct = KafkaJsonToStruct.buildStruct(jsonMap, schema);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                Schema.STRING_SCHEMA,
                key,
                schema,
                struct,
                null,
                headers);
    }



    /* (non-Javadoc)
     * @see org.apache.kafka.connect.source.SourceTask#commitRecord(org.apache.kafka.connect.source.SourceRecord)
     */
//    @Override
//    public void commitRecord(SourceRecord record) throws InterruptedException {
//        Guard.verifyNotNull(record, "record");
//        final String receipt = record.sourceOffset().get(SqsConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue())
//                .toString();
//        log.debug(".commit-record:url={}, receipt-handle={}", config.getQueueUrl(), receipt);
//        client.delete(config.getQueueUrl(), receipt);
//        super.commitRecord(record);
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.kafka.connect.source.SourceTask#stop()
//     */
//    @Override
//    public void stop() {
//        log.info("task.stop:OK");
//    }

    /**
     * Test that we have both the task configuration and SQS client properly
     * initialized.
     *
     * @return true if task is in a valid state.
     */
//    private boolean isValidState() {
//        return null != config && null != client;
//    }

}
