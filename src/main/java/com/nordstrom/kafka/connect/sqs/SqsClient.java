package com.nordstrom.kafka.connect.sqs;

import java.util.List;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsClient {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final software.amazon.awssdk.services.sqs.SqsClient client;

  public SqsClient(SqsConnectorConfig config) {
    client = software.amazon.awssdk.services.sqs.SqsClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(
                    StsAssumeRoleCredentialsProvider.builder()
                            .refreshRequest(b -> b
                                    .roleArn("")
                                    .roleSessionName("")
                                    .durationSeconds(900))
                            .stsClient(StsClient.create())
                            .build())
            .build();
  }

  public void delete(final String url, final String receiptHandle) {
    Guard.verifyValidUrl(url);
    Guard.verifyNotNullOrEmpty(receiptHandle, "receiptHandle");

    final DeleteMessageRequest request = DeleteMessageRequest.builder()
            .queueUrl(url)
            .receiptHandle(receiptHandle)
            .build();

    final DeleteMessageResponse result = client.deleteMessage(request);
    log.debug(".delete:receipt-handle={}, rc={}", receiptHandle, result.sdkHttpResponse().statusCode());
  }

  public List<Message> receive(final String url, final int maxMessages, final int waitTimeSeconds,
                               final Boolean messageAttributesEnabled, final List<String> messageAttributesList) {
    log.debug(".receive:queue={}, max={}, wait={}", url, maxMessages, waitTimeSeconds);

    Guard.verifyValidUrl(url);
    Guard.verifyNonNegative(waitTimeSeconds, "sqs.wait.time.seconds");
    Guard.verifyInRange(maxMessages, 0, 10, "sqs.max.messages");
    if (!isValidState()) {
      throw new IllegalStateException("SQS client is not initialized");
    }

    ReceiveMessageRequest.Builder requestBuilder = ReceiveMessageRequest.builder()
            .queueUrl(url)
            .maxNumberOfMessages(maxMessages)
            .waitTimeSeconds(waitTimeSeconds);

    if (messageAttributesEnabled) {
      if (messageAttributesList.isEmpty()) {
        requestBuilder.messageAttributeNames("All");
      } else {
        requestBuilder.messageAttributeNames(messageAttributesList);
      }
    }

    final ReceiveMessageResponse result = client.receiveMessage(requestBuilder.build());
    log.debug(".receive:{} messages, url={}, rc={}", result.messages().size(), url,
            result.sdkHttpResponse().statusCode());

    return result.messages();
  }

  public String send(final String url, final String body, final String groupId,
                     final String messageId, final Map<String, MessageAttributeValue> messageAttributes) {
    log.debug(".send: queue={}, gid={}, mid={}", url, groupId, messageId);

    Guard.verifyValidUrl(url);
    if (!isValidState()) {
      throw new IllegalStateException("SQS client is not initialized");
    }

    final boolean fifo = isFifo(url);

    SendMessageRequest.Builder requestBuilder = SendMessageRequest.builder()
            .queueUrl(url)
            .messageBody(body);

    if (messageAttributes != null) {
      requestBuilder.messageAttributes(messageAttributes);
    }

    if (fifo) {
      Guard.verifyNotNullOrEmpty(groupId, "groupId");
      Guard.verifyNotNullOrEmpty(messageId, "messageId");
      requestBuilder.messageGroupId(groupId)
              .messageDeduplicationId(messageId);
    }

    final SendMessageResponse result = client.sendMessage(requestBuilder.build());
    log.debug(".send-message.OK: queue={}, result={}", url, result);

    return fifo ? result.sequenceNumber() : result.messageId();
  }

  private boolean isFifo(final String url) {
      String AWS_FIFO_SUFFIX = ".fifo";
      return url.endsWith(AWS_FIFO_SUFFIX);
  }

  private boolean isValidState() {
    return Facility.isNotNull(client);
  }


//  @SuppressWarnings("unchecked")
//  public AWSCredentialsProvider getCredentialsProvider(Map<String, ?> configs) {
//
//    try {
//      Object providerField = configs.get("class");
//      String providerClass = SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_DEFAULT.getValue();
//      if (null != providerField) {
//        providerClass = providerField.toString();
//      }
//      AWSCredentialsProvider provider = ((Class<? extends AWSCredentialsProvider>)
//              getClass(providerClass)).newInstance();
//
//      if (provider instanceof Configurable) {
////        Map<String, Object> configs = originalsWithPrefix(CREDENTIALS_PROVIDER_CONFIG_PREFIX);
////        configs.remove(CREDENTIALS_PROVIDER_CLASS_CONFIG.substring(
////            CREDENTIALS_PROVIDER_CONFIG_PREFIX.length(),
////            CREDENTIALS_PROVIDER_CLASS_CONFIG.length()
////        ));
//        ((Configurable) provider).configure(configs);
//      }
//
//      return provider;
//    } catch (IllegalAccessException | InstantiationException e) {
//      throw new ConnectException(
//              "Invalid class for: " + SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG,
//              e
//      );
//    }
//  }

  public Class<?> getClass(String className) {
    log.warn(".get-class:class={}", className);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      log.error("Provider class not found: {}", e);
    }
    return null;
  }

}
