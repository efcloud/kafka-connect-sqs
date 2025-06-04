package com.nordstrom.kafka.connect.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import com.nordstrom.kafka.connect.sqs.SqsConnectorConfigKeys;
import com.nordstrom.kafka.connect.utils.StringUtils;
import org.apache.kafka.common.Configurable;

import java.net.URI;
import java.util.Map;

public class AWSAssumeRoleCredentialsProvider implements AwsCredentialsProvider, Configurable, AutoCloseable {
  public static final String EXTERNAL_ID_CONFIG = "external.id";
  public static final String ROLE_ARN_CONFIG = "role.arn";
  public static final String SESSION_NAME_CONFIG = "session.name";

  private String externalId;
  private String roleArn;
  private String sessionName;
  private String region;
  private String endpointUrl;
  private StsAssumeRoleCredentialsProvider credentialsProvider;

  @Override
  public void configure(Map<String, ?> map) {
    externalId = getOptionalField(map, EXTERNAL_ID_CONFIG);
    roleArn = getRequiredField(map, ROLE_ARN_CONFIG);
    sessionName = getRequiredField(map, SESSION_NAME_CONFIG);
    region = getRequiredField(map, SqsConnectorConfigKeys.SQS_REGION.getValue());
    endpointUrl = getOptionalField(map, SqsConnectorConfigKeys.SQS_ENDPOINT_URL.getValue());

    StsClient stsClient = StringUtils.isBlank(endpointUrl)
            ? StsClient.builder().region(Region.of(region)).build()
            : StsClient.builder()
            .endpointOverride(URI.create(endpointUrl))
            .region(Region.of(region))
            .build();

    AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName(sessionName)
            .durationSeconds(900);

    if (externalId != null) {
      roleRequestBuilder.externalId(externalId);
    }

    credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
            .refreshRequest(roleRequestBuilder.build())
            .stsClient(stsClient)
            .build();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    if (credentialsProvider == null) {
      throw new IllegalStateException("Credentials Provider not configured – need to call configure() first.");
    }
    return credentialsProvider.resolveCredentials();
  }

  @Override
  public void close() {
    if (credentialsProvider != null) {
      credentialsProvider.close();
    }
  }

  private String getOptionalField(final Map<String, ?> map, final String fieldName) {
    final Object field = map.get(fieldName);
    if (isNotNull(field)) {
      return field.toString();
    }
    return null;
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
