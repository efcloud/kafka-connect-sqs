/*
 * Copyright 2019 Nordstrom, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.nordstrom.kafka.connect.sqs;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import com.nordstrom.kafka.connect.utils.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.errors.ConnectException;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.endpoints.EndpointProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.endpoints.SqsEndpointProvider;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsClient {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final software.amazon.awssdk.services.sqs.SqsClient client;

  public SqsClient(SqsConnectorConfig config) {
    Map<String, Object> credentialProviderConfigs = config.originalsWithPrefix(
            SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CONFIG_PREFIX.getValue());
    credentialProviderConfigs.put(SqsConnectorConfigKeys.SQS_REGION.getValue(), config.getRegion());
    AwsCredentialsProvider provider = null;

    try {
      provider = getCredentialsProvider(credentialProviderConfigs);
    } catch ( Exception e ) {
      log.error("Problem initializing provider", e);
    }

    SqsClientBuilder clientBuilder = software.amazon.awssdk.services.sqs.SqsClient.builder()
            .region(Region.of(config.getRegion()));

    // Only set endpoint override if endpoint URL is not null or empty
    if (!StringUtils.isBlank(config.getEndpointUrl())) {
      clientBuilder.endpointOverride(URI.create(config.getEndpointUrl()));
    }

    // Only set credentials provider if it's not null
    if (provider != null) {
      clientBuilder.credentialsProvider(provider);
    }

    client = clientBuilder.build();
  }

  /**
   * Delete a message from the SQS queue.
   *
   * @param url           SQS queue url.
   * @param receiptHandle Message receipt handle of message to delete.
   */
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

  /**
   * Receive messages from the SQS queue.
   *
   * @param url             SQS queue url.
   * @param maxMessages     Maximum number of messages to receive for this call.
   * @param waitTimeSeconds Time to wait, in seconds, for messages to arrive.
   * @param messageAttributesEnabled Whether to collect message attributes.
   * @param messageAttributesList Which message attributes to collect; if empty, all attributes are collected.
   * @return Collection of messages received.
   */
  public List<Message> receive(final String url, final int maxMessages, final int waitTimeSeconds, final Boolean messageAttributesEnabled, final List<String> messageAttributesList) {
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

  /**
   * Send a message to an SQS queue.
   *
   * @param url       SQS queue url.
   * @param body      The message to send.
   * @param groupId   Optional group identifier (fifo queues only).
   * @param messageId Optional message identifier (fifo queues only).
   * @param messageAttributes The message attributes to send.
   * @return Sequence number when FIFO; otherwise, the message identifier
   */
  public String send(final String url, final String body, final String groupId, final String messageId, final Map<String, MessageAttributeValue> messageAttributes) {
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

  /**
   * Test that we have properly initialized the AWS SQS client.
   *
   * @return true if client is in a valid state.
   */
  private boolean isValidState() {
    return Facility.isNotNull(client);
  }

  @SuppressWarnings("unchecked")
  public AwsCredentialsProvider getCredentialsProvider(Map<String, ?> configs) {

      String providerClass = null;
      try {
          Object providerField = configs.get("class");
          providerClass = SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_DEFAULT.getValue();
          if (null != providerField) {
              providerClass = providerField.toString();
          }
          AwsCredentialsProvider provider = ((Class<? extends AwsCredentialsProvider>)
                  getClass(providerClass)).getDeclaredConstructor().newInstance();

          if (provider instanceof Configurable) {
              ((Configurable) provider).configure(configs);
          }

          return provider;
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
          throw new ConnectException(
                  "Invalid class for: " + SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG,
                  e
          );
      } catch (ClassNotFoundException e) {
          throw new ConnectException(
                  "Provider class not found: " + providerClass, e
          );
      } catch (Exception e) {
        throw new ConnectException(
                "Unknown exception occurred while building SQS client", e
        );
      }
  }

  public Class<?> getClass(String className) throws ClassNotFoundException {
    log.warn(".get-class:class={}", className);
    return Class.forName(className);
  }

}
