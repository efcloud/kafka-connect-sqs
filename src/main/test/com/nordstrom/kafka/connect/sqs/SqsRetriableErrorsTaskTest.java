package com.nordstrom.kafka.connect.sqs;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqsRetriableErrorsTaskTest {

    private static final String QUEUE_URL = "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/queue";

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), QUEUE_URL);
        props.put(SqsConnectorConfigKeys.TOPICS.getValue(), "topic");
        props.put(SqsConnectorConfigKeys.SQS_REGION.getValue(), "cn-north-1");
        return props;
    }

    private static SqsSinkConnectorTask sinkTask(SqsClient client, SinkTaskContext context) {
        SqsSinkConnectorTask task = new SqsSinkConnectorTask();
        task.initialize(context);
        task.client = client;
        task.config = new SqsSinkConnectorConfig(props());
        return task;
    }

    private static List<SinkRecord> oneRecord() {
        return Collections.singletonList(
                new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "value", 0));
    }

    @Test
    void sinkTurnsTransientSendErrorIntoRetriableException() {
        SqsClient client = mock(SqsClient.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        SdkClientException cause = SdkClientException.create("Unable to execute HTTP request");
        when(client.send(anyString(), anyString(), anyString(), anyString(), any())).thenThrow(cause);

        RetriableException e = assertThrows(RetriableException.class,
                () -> sinkTask(client, context).put(oneRecord()));

        assertSame(cause, e.getCause());
        verify(context).timeout(SqsErrors.RETRY_BACKOFF_MS);
    }

    @Test
    void sinkRethrowsPermanentSendError() {
        SqsClient client = mock(SqsClient.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        RuntimeException cause = SqsErrorsTest.serviceError(403, "AccessDenied");
        when(client.send(anyString(), anyString(), anyString(), anyString(), any())).thenThrow(cause);

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> sinkTask(client, context).put(oneRecord()));

        assertSame(cause, e);
        verify(context, never()).timeout(any(Long.class));
    }

    private static SqsSourceConnectorTask sourceTask(SqsClient client) {
        SqsSourceConnectorTask task = new SqsSourceConnectorTask();
        task.client = client;
        task.config = new SqsSourceConnectorConfig(props());
        task.retryBackoffMs = 0;
        return task;
    }

    @Test
    void sourceTurnsTransientReceiveErrorIntoRetriableException() {
        SqsClient client = mock(SqsClient.class);
        SdkClientException cause = SdkClientException.create("Unable to execute HTTP request");
        when(client.receive(anyString(), anyInt(), anyInt(), anyBoolean(), any())).thenThrow(cause);

        RetriableException e = assertThrows(RetriableException.class, () -> sourceTask(client).poll());

        assertSame(cause, e.getCause());
    }

    @Test
    void sourceRethrowsPermanentReceiveError() {
        SqsClient client = mock(SqsClient.class);
        RuntimeException cause = SqsErrorsTest.serviceError(403, "AccessDenied");
        when(client.receive(anyString(), anyInt(), anyInt(), anyBoolean(), any())).thenThrow(cause);

        RuntimeException e = assertThrows(RuntimeException.class, () -> sourceTask(client).poll());

        assertSame(cause, e);
    }
}
