/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.s3a;

import alluxio.retry.CountingRetry;
import alluxio.underfs.s3a.S3ARetryClient.S3RetryHandler;
import alluxio.underfs.s3a.S3ARetryClient.ThrowingSupplier;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Supplier;

public class S3ARetryClientTest {

  public static class ErrorThrowingClient extends AbstractAmazonS3 {
    private final ThrowingSupplier<ObjectListing> mOnCall;

    public ErrorThrowingClient(ThrowingSupplier<ObjectListing> call) {
      mOnCall = call;
    }

    @Override
    public ObjectListing listObjects(String bucketName) throws SdkClientException,
        AmazonServiceException {
      return mOnCall.supply();
    }
  }

  @Test
  public void rateLimitExceededRetryHandlerRetryFails() throws Exception {
    final int[] numOps = {0};
    final int numFails = 5;
    ErrorThrowingClient errorThrowingClient = new ErrorThrowingClient(() -> {
      if (numOps[0]++ < numFails) {
        AmazonS3Exception exception = new AmazonS3Exception("Too many requests");
        exception.setErrorCode("SlowDown");
        throw exception;
      }
      return new ObjectListing();
    });
    Supplier<S3RetryHandler> handler = () -> new S3ARetryClient.RateLimitExceededRetryHandler(
        new CountingRetry(numFails - 1),
        S3ARetryClient.NoRetryHandler.INSTANCE
    );
    S3ARetryClient client = new S3ARetryClient(errorThrowingClient, handler);

    Assert.assertThrows(AmazonS3Exception.class, () -> client.listObjects("someBucket"));
  }

  @Test
  public void rateLimitExceededRetryHandlerRetryOK() throws Exception {
    final int[] numOps = {0};
    final int numFails = 5;
    ErrorThrowingClient errorThrowingClient = new ErrorThrowingClient(() -> {
      if (numOps[0]++ < numFails) {
        AmazonS3Exception exception = new AmazonS3Exception("Too many requests");
        exception.setErrorCode("SlowDown");
        throw exception;
      }
      return new ObjectListing();
    });
    Supplier<S3RetryHandler> handler = () -> new S3ARetryClient.RateLimitExceededRetryHandler(
        new CountingRetry(numFails),
        S3ARetryClient.NoRetryHandler.INSTANCE
    );
    S3ARetryClient client = new S3ARetryClient(errorThrowingClient, handler);

    Assert.assertNotNull(client.listObjects("someBucket"));
  }

  @Test
  public void twoCallsHaveIndependentRetryContexts() throws Exception {
    final int[] numOps = {0};
    final int numFails = 2;
    ErrorThrowingClient errorThrowingClient = new ErrorThrowingClient(() -> {
      if (numOps[0]++ < numFails) {
        AmazonS3Exception exception = new AmazonS3Exception("Too many requests");
        exception.setErrorCode("SlowDown");
        throw exception;
      }
      return new ObjectListing();
    });
    Supplier<S3RetryHandler> handler = () -> new S3ARetryClient.RateLimitExceededRetryHandler(
        new CountingRetry(0),
        S3ARetryClient.NoRetryHandler.INSTANCE
    );
    S3ARetryClient client = new S3ARetryClient(errorThrowingClient, handler);

    Assert.assertThrows(AmazonS3Exception.class, () -> client.listObjects("someBucket"));
    Assert.assertThrows(AmazonS3Exception.class, () -> client.listObjects("someBucket"));
    Assert.assertNotNull(client.listObjects("someBucket"));
  }
}
