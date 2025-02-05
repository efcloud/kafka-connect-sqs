package com.nordstrom.kafka.connect.sqs;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SqsSinkConnectorTaskTest {

    @Mock
    private SqsClient mockClient;

    @Mock
    private SqsSinkConnectorConfig mockConfig;

    @Mock
    private Logger mockLogger;

    private SqsSinkConnectorTask task;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        task = new SqsSinkConnectorTask();
        task.client = mockClient;

        Map<String, String> configMap = new HashMap<>();
        configMap.put(SqsConnectorConfigKeys.VALUE_TRANSFORM_TO_JSON.getValue(), "true");
        configMap.put(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), "http://example.com/queue");
        configMap.put(SqsConnectorConfigKeys.TOPICS.getValue(), "test-topic");
        task.config = new SqsSinkConnectorConfig(configMap);
    }

    @Test
    public void testPutWithStructValue() throws Exception {
        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("field1", "test")
                .put("field2", 123);

        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", schema, struct, 0);
        Collection<SinkRecord> records = Collections.singletonList(record);

        task.put(records);

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockClient).send(anyString(), bodyCaptor.capture(), anyString(), anyString(), any());
        String body = bodyCaptor.getValue();
        System.out.println(body);
        assertEquals("{\"field1\":\"test\",\"field2\":123}", body);
    }

    @Test
    public void testPutWithNestedStructValue() throws Exception {
        Schema nestedSchema = SchemaBuilder.struct()
                .field("nestedField1", Schema.STRING_SCHEMA)
                .field("nestedField2", Schema.INT32_SCHEMA)
                .build();

        Struct nestedStruct = new Struct(nestedSchema)
                .put("nestedField1", "nestedTest")
                .put("nestedField2", 456);

        Schema mainSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .field("nestedStruct", nestedSchema)
                .build();

        Struct mainStruct = new Struct(mainSchema)
                .put("field1", "test")
                .put("field2", 123)
                .put("nestedStruct", nestedStruct);

        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", mainSchema, mainStruct, 0);
        Collection<SinkRecord> records = Collections.singletonList(record);

        task.put(records);

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockClient).send(anyString(), bodyCaptor.capture(), anyString(), anyString(), any());
        String body = bodyCaptor.getValue();
        System.out.println(body);
        assertEquals("{\"field1\":\"test\",\"field2\":123,\"nestedStruct\":{\"nestedField1\":\"nestedTest\",\"nestedField2\":456}}", body);
    }

    @Test
    public void testPutEmptyRecords() {
        Collection<SinkRecord> records = Collections.emptyList();
        task.put(records);
        verify(mockClient, never()).send(anyString(), anyString(), anyString(), anyString(), anyMap());
    }
}

