package com.nordstrom.kafka.connect.sqs;

import com.nordstrom.kafka.connect.utils.ParseException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class SqsSourceConnectorTaskTest {

    Map<String, String> sourcePartition = Collections.singletonMap("partitionKey", "partitionValue");
    Map<String, String> sourceOffset = Collections.singletonMap("offsetKey", "offsetValue");
    String topic = "testTopic";
    String key = "testKey";
    ConnectHeaders headers = new ConnectHeaders();

    @Test
    void parseJSONString() {
        String body = "{\"field1\":\"value1\",\"field2\":\"test\"}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        Struct value = (Struct) result.value();
        assertEquals("value1", value.getString("field1"));
        assertEquals("test", value.getString("field2"));
    }
    
    @Test
    void parseJSONInteger() {
        String body = "{\"field1\":\"value1\",\"field2\":2}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        Struct value = (Struct) result.value();
        assertEquals("value1", value.getString("field1"));
        assertEquals(2, value.getInt32("field2"));
    }

    @Test
    void parseJSONFloat() {
        String body = "{\"field1\":\"value1\",\"field2\":2.3}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        Struct value = (Struct) result.value();
        assertEquals("value1", value.getString("field1"));
        assertEquals(2.3, value.getFloat64("field2"));
    }

    @Test
    void parseJSONDateTimeISO8601() {
        String body = "{\"field1\":\"value1\",\"field2\":\"2023-10-01T10:20:30Z\"}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        Struct value = (Struct) result.value();
        assertEquals("value1", value.getString("field1"));
        assertEquals(Timestamp.from(Instant.parse("2023-10-01T10:20:30Z")), value.get("field2"));
    }

    @Test
    void parseJSONDateTime() {
        String body = "{\"field1\":\"value1\",\"field2\":\"2023-10-01 10:20:30\"}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        Struct value = (Struct) result.value();
        assertEquals("value1", value.getString("field1"));
        assertEquals(Timestamp.from(Instant.parse("2023-10-01T10:20:30Z")), value.get("field2"));
    }

    @Test
    void parseJSONEmptyJson() {
        String body = "{}";
        SourceRecord result = SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);

        assertNotNull(result);
        Struct value = (Struct) result.value();
        assertTrue(value.schema().fields().isEmpty());
    }

    @Test
    void parseJSONInvalidJson() {
        String body = "{invalidJson}";
        assertThrows(ParseException.class, () -> {
            SqsSourceConnectorTask.parseJSON(body, sourcePartition, sourceOffset, topic, key, headers);
        });
    }
}