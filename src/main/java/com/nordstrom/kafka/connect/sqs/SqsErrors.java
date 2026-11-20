package com.nordstrom.kafka.connect.sqs;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Decides which AWS SDK failures are transient, so the tasks can hand them to Kafka Connect as
 * {@link org.apache.kafka.connect.errors.RetriableException} instead of failing permanently.
 */
final class SqsErrors {

  /** How long to wait before Kafka Connect retries a call that failed with a transient error. */
  static final long RETRY_BACKOFF_MS = 5000L;

  private SqsErrors() {
  }

  /**
   * Transient: client-side failures (connection errors, timeouts, credential refresh that could not
   * reach STS), throttling, and 5xx responses. Anything else, such as access denied or a missing
   * queue, will not fix itself by retrying.
   */
  static boolean isRetriable(Throwable e) {
    if (e instanceof SdkClientException) {
      return true;
    }
    if (e instanceof AwsServiceException) {
      AwsServiceException ase = (AwsServiceException) e;
      return ase.isThrottlingException() || ase.statusCode() >= 500;
    }
    return false;
  }
}
