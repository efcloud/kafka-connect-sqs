/*
 * Copyright 2019 Nordstrom, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nordstrom.kafka.connect.sqs;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class SqsSinkConnectorConfig extends SqsConnectorConfig {
  private final Boolean messageAttributesEnabled;
  private final List<String> messageAttributesList;

  private final String timestampFormat;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), Type.STRING, Importance.HIGH, "URL of the SQS queue to be written to.")
      .define(SqsConnectorConfigKeys.TOPICS.getValue(), Type.STRING, Importance.HIGH, "Kafka topic to be read from.")
      .define(SqsConnectorConfigKeys.SQS_REGION.getValue(), Type.STRING, System.getenv("AWS_REGION"), Importance.HIGH,
          "SQS queue AWS region.")
      .define(SqsConnectorConfigKeys.SQS_ENDPOINT_URL.getValue(), Type.STRING, "", Importance.LOW,
          "If specified, the connector will override the AWS region specific endpoint URL with this value. Note that this is not the queue URL.")
      .define(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getValue(), Type.CLASS,
          SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_DEFAULT.getValue(),
          new CredentialsProviderValidator(),
          Importance.LOW,
          "Credentials provider or provider chain to use for authentication to AWS. By default the connector uses 'DefaultCredentialsProvider'.",
          "SQS",
          0,
          ConfigDef.Width.LONG,
          "AWS Credentials Provider Class")
      .define(SqsConnectorConfigKeys.SQS_MESSAGE_ATTRIBUTES_ENABLED.getValue(), Type.BOOLEAN, false, Importance.LOW,
          "If true, it gets the Kafka Headers and inserts them as SQS MessageAttributes (only string headers are currently supported). Default is false.")
      .define(SqsConnectorConfigKeys.SQS_MESSAGE_ATTRIBUTES_INCLUDE_LIST.getValue(), Type.LIST, "", Importance.LOW,
          "The comma separated list of Header names to be included, if empty it includes all the Headers. Default is the empty string.")
      .define(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_ACCESS_KEY_ID.getValue(), Type.STRING, "", Importance.LOW,
          "AWS Secret Access Key to be used with Config credentials provider.")
      .define(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY.getValue(), Type.PASSWORD, "", Importance.LOW,
          "AWS Secret Access Key to be used with Config credentials provider")
      .define(SqsConnectorConfigKeys.VALUE_TRANSFORM_TO_JSON.getValue(), Type.BOOLEAN, false, Importance.LOW,
          "If true, a transformation is applied to the value of the Kafka message to convert it to a JSON string. Default is false.")
      .define(SqsConnectorConfigKeys.VALUE_TRANSFORM_TIMESTAMP_FORMAT.getValue(), Type.STRING, "yyyy-MM-dd'T'HH:mm:ss'Z'", Importance.LOW,
          String.format("When %s is true, Timestamps are transformed into a string using this format. Default is yyyy-MM-dd'T'HH:mm:ss'Z'.", SqsConnectorConfigKeys.VALUE_TRANSFORM_TO_JSON.getValue()));

  public static ConfigDef config() {
    return CONFIG_DEF;
  }

  public SqsSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    timestampFormat = getString(SqsConnectorConfigKeys.VALUE_TRANSFORM_TIMESTAMP_FORMAT.getValue());

    messageAttributesEnabled = getBoolean(SqsConnectorConfigKeys.SQS_MESSAGE_ATTRIBUTES_ENABLED.getValue());
    if (messageAttributesEnabled) {
      messageAttributesList = getList(SqsConnectorConfigKeys.SQS_MESSAGE_ATTRIBUTES_INCLUDE_LIST.getValue());
    } else {
      messageAttributesList = Collections.emptyList();
    }
  }

  public Boolean getMessageAttributesEnabled() {
    return messageAttributesEnabled;
  }

  public List<String> getMessageAttributesList() {
    return messageAttributesList;
  }

  public String getTimestampFormat() { return timestampFormat; }

}
