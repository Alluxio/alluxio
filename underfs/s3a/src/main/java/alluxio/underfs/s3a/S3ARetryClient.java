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

import alluxio.retry.RetryPolicy;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionResult;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusResult;
import com.amazonaws.services.s3.model.GetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionResult;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.GetS3AccountOwnerRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfVersionsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PresignedUrlDownloadRequest;
import com.amazonaws.services.s3.model.PresignedUrlDownloadResult;
import com.amazonaws.services.s3.model.PresignedUrlUploadRequest;
import com.amazonaws.services.s3.model.PresignedUrlUploadResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionResult;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.SetObjectRetentionRequest;
import com.amazonaws.services.s3.model.SetObjectRetentionResult;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

public class S3ARetryClient extends AbstractAmazonS3 {
  public interface S3RetryHandler {
    boolean shouldRetry(AmazonClientException exception);
  }

  public static abstract class FallbackS3RetryHandler implements S3RetryHandler {
    protected final S3RetryHandler mFallback;

    protected FallbackS3RetryHandler(S3RetryHandler fallback) {
      mFallback = fallback;
    }

    @Override
    public boolean shouldRetry(AmazonClientException exception) {
      return mFallback.shouldRetry(exception);
    }
  }

  public static class NoRetryHandler implements S3RetryHandler {
    public static final S3RetryHandler INSTANCE = new NoRetryHandler();

    @Override
    public boolean shouldRetry(AmazonClientException exception) {
      return false;
    }

    private NoRetryHandler() { }
  }

  public static class RateLimitExceededRetryHandler extends FallbackS3RetryHandler {
    private final RetryPolicy mRetryPolicy;

    public RateLimitExceededRetryHandler(RetryPolicy policy, S3RetryHandler fallback) {
      super(fallback);
      // consume the first attempt which never blocks
      policy.attempt();
      mRetryPolicy = policy;
    }

    @Override
    public boolean shouldRetry(AmazonClientException exception) {
      if (exception instanceof AmazonS3Exception) {
        AmazonS3Exception s3Exception = (AmazonS3Exception) exception;
        if (s3Exception.getErrorCode().equals("SlowDown")) {
          return mRetryPolicy.attempt();
        }
      }
      return super.shouldRetry(exception);
    }
  }

  public interface ThrowingSupplier<R> {
    R supply() throws AmazonClientException;
  }

  protected final AmazonS3 mDelegate;
  protected final Supplier<S3RetryHandler> mHandlerSupplier;

  public S3ARetryClient(AmazonS3 delegate, Supplier<S3RetryHandler> handlerSupplier) {
    mDelegate = delegate;
    mHandlerSupplier = handlerSupplier;
  }

  protected <R> R retry(ThrowingSupplier<R> supplier) {
    S3RetryHandler handler = mHandlerSupplier.get();
    while (true) {
      try {
        return supplier.supply();
      } catch (AmazonClientException e) {
        if (!handler.shouldRetry(e)) {
          throw e;
        }
      }
    }
  }

  @Override
  public void setEndpoint(String endpoint) {
    retry(() -> {
      mDelegate.setEndpoint(endpoint);
      return null;
    });
  }

  @Override
  public void setRegion(Region region) throws IllegalArgumentException {
    retry(() -> {
      mDelegate.setRegion(region);
      return null;
    });
  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
    retry(() -> {
      mDelegate.setS3ClientOptions(clientOptions);
      return null;
    });
  }

  @Override
  public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.changeObjectStorageClass(bucketName, key, newStorageClass);
      return null;
    });
  }

  @Override
  public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectRedirectLocation(bucketName, key, newRedirectLocation);
      return null;
    });
  }

  @Override
  public ObjectListing listObjects(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjects(bucketName));
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjects(bucketName, prefix));
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjects(listObjectsRequest));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjectsV2(bucketName));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName, String prefix)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjectsV2(bucketName, prefix));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listObjectsV2(listObjectsV2Request));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listNextBatchOfObjects(previousObjectListing));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(
      ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listNextBatchOfObjects(listNextBatchOfObjectsRequest));
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listVersions(bucketName, prefix));
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listNextBatchOfVersions(previousVersionListing));
  }

  @Override
  public VersionListing listNextBatchOfVersions(
      ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listNextBatchOfVersions(listNextBatchOfVersionsRequest));

  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix, String keyMarker,
                                     String versionIdMarker, String delimiter, Integer maxResults)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listVersions(
        bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxResults));
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listVersions(listVersionsRequest));
  }

  @Override
  public Owner getS3AccountOwner() throws SdkClientException, AmazonServiceException {
    return retry(mDelegate::getS3AccountOwner);
  }

  @Override
  public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getS3AccountOwner(getS3AccountOwnerRequest));
  }

  @Override
  public boolean doesBucketExist(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.doesBucketExist(bucketName));
  }

  @Override
  public boolean doesBucketExistV2(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.doesBucketExistV2(bucketName));
  }

  @Override
  public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.headBucket(headBucketRequest));
  }

  @Override
  public List<Bucket> listBuckets() throws SdkClientException, AmazonServiceException {
    return retry(mDelegate::listBuckets);
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.listBuckets(listBucketsRequest));
  }

  @Override
  public String getBucketLocation(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketLocation(bucketName));
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketLocation(getBucketLocationRequest));
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.createBucket(createBucketRequest));
  }

  @Override
  public Bucket createBucket(String bucketName) throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.createBucket(bucketName));
  }

  @Override
  public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.createBucket(bucketName, region));
  }

  @Override
  public Bucket createBucket(String bucketName, String region)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.createBucket(bucketName, region));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObjectAcl(bucketName, key));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObjectAcl(bucketName, key, versionId));
  }

  @Override
  public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObjectAcl(getObjectAclRequest));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectAcl(bucketName, key, acl);
      return null;
    });
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectAcl(bucketName, key, acl);
      return null;
    });
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectAcl(bucketName, key, versionId, acl);
      return null;
    });
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId,
                           CannedAccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectAcl(bucketName, key, versionId, acl);
      return null;
    });
  }

  @Override
  public void setObjectAcl(SetObjectAclRequest setObjectAclRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setObjectAcl(setObjectAclRequest);
      return null;
    });
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketAcl(bucketName));
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketAcl(setBucketAclRequest);
      return null;
    });
  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketAcl(getBucketAclRequest));
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketAcl(bucketName, acl);
      return null;
    });
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList acl)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketAcl(bucketName, acl);
      return null;
    });
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObjectMetadata(bucketName, key));
  }

  @Override
  public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObjectMetadata(getObjectMetadataRequest));
  }

  @Override
  public S3Object getObject(String bucketName, String key)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObject(bucketName, key));
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObject(getObjectRequest));
  }

  @Override
  public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getObject(getObjectRequest, destinationFile));
  }

  @Override
  public String getObjectAsString(String bucketName, String key)
      throws AmazonServiceException, SdkClientException {
    return retry(() -> mDelegate.getObjectAsString(bucketName, key));
  }

  @Override
  public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest) {
    return retry(() -> mDelegate.getObjectTagging(getObjectTaggingRequest));
  }

  @Override
  public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest) {
    return retry(() -> mDelegate.setObjectTagging(setObjectTaggingRequest));
  }

  @Override
  public DeleteObjectTaggingResult deleteObjectTagging(
      DeleteObjectTaggingRequest deleteObjectTaggingRequest) {
    return retry(() -> mDelegate.deleteObjectTagging(deleteObjectTaggingRequest));
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucket(deleteBucketRequest);
      return null;
    });
  }

  @Override
  public void deleteBucket(String bucketName) throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucket(bucketName);
      return null;
    });
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.putObject(putObjectRequest));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.putObject(bucketName, key, file));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, InputStream input,
                                   ObjectMetadata metadata)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.putObject(bucketName, key, input, metadata));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, String content)
      throws AmazonServiceException, SdkClientException {
    return retry(() -> mDelegate.putObject(bucketName, key, content));
  }

  @Override
  public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
                                     String destinationBucketName, String destinationKey)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.copyObject(
        sourceBucketName, sourceKey, destinationBucketName, destinationKey));
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.copyObject(copyObjectRequest));
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.copyPart(copyPartRequest));
  }

  @Override
  public void deleteObject(String bucketName, String key)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteObject(bucketName, key);
      return null;
    });
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteObject(deleteObjectRequest);
      return null;
    });
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.deleteObjects(deleteObjectsRequest));
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteVersion(bucketName, key, versionId);
      return null;
    });
  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteVersion(deleteVersionRequest);
      return null;
    });
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketLoggingConfiguration(bucketName));
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(
      GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketLoggingConfiguration(getBucketLoggingConfigurationRequest));

  }

  @Override
  public void setBucketLoggingConfiguration(
      SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketLoggingConfiguration(setBucketLoggingConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketVersioningConfiguration(bucketName));
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(
      GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketVersioningConfiguration(getBucketVersioningConfigurationRequest));
  }

  @Override
  public void setBucketVersioningConfiguration(
      SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    return retry(() -> mDelegate.getBucketLifecycleConfiguration(bucketName));
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(
      GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
    return retry(() ->
        mDelegate.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest));

  }

  @Override
  public void setBucketLifecycleConfiguration(String bucketName,
                                              BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    retry(() -> {
      mDelegate.setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration);
      return null;
    });
  }

  @Override
  public void setBucketLifecycleConfiguration(
      SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
    retry(() -> {
      mDelegate.setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest);
      return null;
    });
  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
    retry(() -> {
      mDelegate.deleteBucketLifecycleConfiguration(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketLifecycleConfiguration(
      DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
    retry(() -> {
      mDelegate.deleteBucketLifecycleConfiguration(deleteBucketLifecycleConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
    return retry(() -> mDelegate.getBucketCrossOriginConfiguration(bucketName));
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(
      GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest) {
    return retry(() ->
        mDelegate.getBucketCrossOriginConfiguration(getBucketCrossOriginConfigurationRequest));
  }

  @Override
  public void setBucketCrossOriginConfiguration(String bucketName,
                                                BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
    retry(() -> {
      mDelegate.setBucketCrossOriginConfiguration(bucketName, bucketCrossOriginConfiguration);
      return null;
    });
  }

  @Override
  public void setBucketCrossOriginConfiguration(
      SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
    retry(() -> {
      mDelegate.setBucketCrossOriginConfiguration(setBucketCrossOriginConfigurationRequest);
      return null;
    });
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
    retry(() -> {
      mDelegate.deleteBucketCrossOriginConfiguration(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(
      DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
    retry(() -> {
      mDelegate.deleteBucketCrossOriginConfiguration(deleteBucketCrossOriginConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    return retry(() -> mDelegate.getBucketTaggingConfiguration(bucketName));
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(
      GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest) {
    return retry(() ->
        mDelegate.getBucketTaggingConfiguration(getBucketTaggingConfigurationRequest));
  }

  @Override
  public void setBucketTaggingConfiguration(String bucketName,
                                            BucketTaggingConfiguration bucketTaggingConfiguration) {
    retry(() -> {
      mDelegate.setBucketTaggingConfiguration(bucketName, bucketTaggingConfiguration);
      return null;
    });
  }

  @Override
  public void setBucketTaggingConfiguration(
      SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
    retry(() -> {
      mDelegate.setBucketTaggingConfiguration(setBucketTaggingConfigurationRequest);
      return null;
    });
  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
    retry(() -> {
      mDelegate.deleteBucketTaggingConfiguration(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketTaggingConfiguration(
      DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
    retry(() -> {
      mDelegate.deleteBucketTaggingConfiguration(deleteBucketTaggingConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() -> mDelegate.getBucketNotificationConfiguration(bucketName));
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(
      GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest));
  }

  @Override
  public void setBucketNotificationConfiguration(
      SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketNotificationConfiguration(setBucketNotificationConfigurationRequest);
      return null;
    });
  }

  @Override
  public void setBucketNotificationConfiguration(String bucketName,
                                                 BucketNotificationConfiguration bucketNotificationConfiguration)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketNotificationConfiguration(bucketName, bucketNotificationConfiguration);
      return null;
    });
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketWebsiteConfiguration(bucketName));
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
      GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketWebsiteConfiguration(getBucketWebsiteConfigurationRequest));
  }

  @Override
  public void setBucketWebsiteConfiguration(String bucketName,
                                            BucketWebsiteConfiguration configuration)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketWebsiteConfiguration(bucketName, configuration);
      return null;
    });
  }

  @Override
  public void setBucketWebsiteConfiguration(
      SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketWebsiteConfiguration(setBucketWebsiteConfigurationRequest);
      return null;
    });
  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucketWebsiteConfiguration(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketWebsiteConfiguration(
      DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucketWebsiteConfiguration(deleteBucketWebsiteConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketPolicy(bucketName));
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.getBucketPolicy(getBucketPolicyRequest));
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketPolicy(bucketName, policyText);
      return null;
    });
  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.setBucketPolicy(setBucketPolicyRequest);
      return null;
    });
  }

  @Override
  public void deleteBucketPolicy(String bucketName)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucketPolicy(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.deleteBucketPolicy(deleteBucketPolicyRequest);
      return null;
    });
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration)
      throws SdkClientException {
    return retry(() ->
        mDelegate.generatePresignedUrl(bucketName, key, expiration));
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
      throws SdkClientException {
    return retry(() ->
        mDelegate.generatePresignedUrl(bucketName, key, expiration, method));
  }

  @Override
  public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest)
      throws SdkClientException {
    return retry(() ->
        mDelegate.generatePresignedUrl(generatePresignedUrlRequest));
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(
      InitiateMultipartUploadRequest request) throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.initiateMultipartUpload(request));
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.uploadPart(request));
  }

  @Override
  public PartListing listParts(ListPartsRequest request)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.listParts(request));
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request)
      throws SdkClientException, AmazonServiceException {
    retry(() -> {
      mDelegate.abortMultipartUpload(request);
      return null;
    });
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(
      CompleteMultipartUploadRequest request) throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.completeMultipartUpload(request));
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
      throws SdkClientException, AmazonServiceException {
    return retry(() ->
        mDelegate.listMultipartUploads(request));
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return retry(() ->
        mDelegate.getCachedResponseMetadata(request));
  }

  @Override
  public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
    retry(() -> {
      mDelegate.restoreObject(request);
      return null;
    });
  }

  @Override
  public RestoreObjectResult restoreObjectV2(RestoreObjectRequest request)
      throws AmazonServiceException {
    return retry(() ->
        mDelegate.restoreObjectV2(request));
  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays)
      throws AmazonServiceException {
    retry(() -> {
      mDelegate.restoreObject(bucketName, key, expirationInDays);
      return null;
    });
  }

  @Override
  public void enableRequesterPays(String bucketName)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.enableRequesterPays(bucketName);
      return null;
    });
  }

  @Override
  public void disableRequesterPays(String bucketName)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.disableRequesterPays(bucketName);
      return null;
    });
  }

  @Override
  public boolean isRequesterPaysEnabled(String bucketName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.isRequesterPaysEnabled(bucketName));
  }

  @Override
  public void setBucketReplicationConfiguration(String bucketName,
                                                BucketReplicationConfiguration configuration)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.setBucketReplicationConfiguration(bucketName, configuration);
      return null;
    });
  }

  @Override
  public void setBucketReplicationConfiguration(
      SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.setBucketReplicationConfiguration(setBucketReplicationConfigurationRequest);
      return null;
    });
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketReplicationConfiguration(bucketName));
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(
      GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketReplicationConfiguration(getBucketReplicationConfigurationRequest));
  }

  @Override
  public void deleteBucketReplicationConfiguration(String bucketName)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.deleteBucketReplicationConfiguration(bucketName);
      return null;
    });
  }

  @Override
  public void deleteBucketReplicationConfiguration(
      DeleteBucketReplicationConfigurationRequest request)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.deleteBucketReplicationConfiguration(request);
      return null;
    });
  }

  @Override
  public boolean doesObjectExist(String bucketName, String objectName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.doesObjectExist(bucketName, objectName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String bucketName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketAccelerateConfiguration(bucketName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
      GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest));
  }

  @Override
  public void setBucketAccelerateConfiguration(String bucketName,
                                               BucketAccelerateConfiguration accelerateConfiguration)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.setBucketAccelerateConfiguration(bucketName, accelerateConfiguration);
      return null;
    });
  }

  @Override
  public void setBucketAccelerateConfiguration(
      SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    retry(() -> {
      mDelegate.setBucketAccelerateConfiguration(setBucketAccelerateConfigurationRequest);
      return null;
    });
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String bucketName,
                                                                                 String id)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
      DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest));
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String bucketName,
                                                                           String id)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
      GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest));
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String bucketName,
                                                                           MetricsConfiguration metricsConfiguration)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketMetricsConfiguration(bucketName, metricsConfiguration));
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
      SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketMetricsConfiguration(setBucketMetricsConfigurationRequest));
  }

  @Override
  public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(
      ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest));
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
      String bucketName, String id) throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
      DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest));
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String bucketName,
                                                                               String id)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
      GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest));
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String bucketName,
                                                                               AnalyticsConfiguration analyticsConfiguration)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketAnalyticsConfiguration(bucketName, analyticsConfiguration));
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
      SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketAnalyticsConfiguration(setBucketAnalyticsConfigurationRequest));
  }

  @Override
  public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(
      ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest));
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
      String bucketName, String id) throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
      DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest));
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String bucketName,
                                                                               String id)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
      GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest));
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String bucketName,
                                                                               InventoryConfiguration inventoryConfiguration)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketInventoryConfiguration(bucketName, inventoryConfiguration));
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
      SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketInventoryConfiguration(setBucketInventoryConfigurationRequest));
  }

  @Override
  public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(
      ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest));
  }

  @Override
  public DeleteBucketEncryptionResult deleteBucketEncryption(String bucketName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketEncryption(bucketName));
  }

  @Override
  public DeleteBucketEncryptionResult deleteBucketEncryption(DeleteBucketEncryptionRequest request)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.deleteBucketEncryption(request));
  }

  @Override
  public GetBucketEncryptionResult getBucketEncryption(String bucketName)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketEncryption(bucketName));
  }

  @Override
  public GetBucketEncryptionResult getBucketEncryption(GetBucketEncryptionRequest request)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.getBucketEncryption(request));
  }

  @Override
  public SetBucketEncryptionResult setBucketEncryption(
      SetBucketEncryptionRequest setBucketEncryptionRequest)
      throws AmazonServiceException, SdkClientException {
    return retry(() ->
        mDelegate.setBucketEncryption(setBucketEncryptionRequest));
  }

  @Override
  public SetPublicAccessBlockResult setPublicAccessBlock(SetPublicAccessBlockRequest request) {
    return retry(() ->
        mDelegate.setPublicAccessBlock(request));
  }

  @Override
  public GetPublicAccessBlockResult getPublicAccessBlock(GetPublicAccessBlockRequest request) {
    return retry(() ->
        mDelegate.getPublicAccessBlock(request));
  }

  @Override
  public DeletePublicAccessBlockResult deletePublicAccessBlock(
      DeletePublicAccessBlockRequest request) {
    return retry(() ->
        mDelegate.deletePublicAccessBlock(request));
  }

  @Override
  public GetBucketPolicyStatusResult getBucketPolicyStatus(GetBucketPolicyStatusRequest request) {
    return retry(() ->
        mDelegate.getBucketPolicyStatus(request));
  }

  @Override
  public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest selectRequest)
      throws AmazonServiceException, SdkClientException {
      return retry(() ->
          mDelegate.selectObjectContent(selectRequest));
  }

  @Override
  public SetObjectLegalHoldResult setObjectLegalHold(
      SetObjectLegalHoldRequest setObjectLegalHoldRequest) {
    return retry(() ->
        mDelegate.setObjectLegalHold(setObjectLegalHoldRequest));
  }

  @Override
  public GetObjectLegalHoldResult getObjectLegalHold(
      GetObjectLegalHoldRequest getObjectLegalHoldRequest) {
    return retry(() ->
        mDelegate.getObjectLegalHold(getObjectLegalHoldRequest));
  }

  @Override
  public SetObjectLockConfigurationResult setObjectLockConfiguration(
      SetObjectLockConfigurationRequest setObjectLockConfigurationRequest) {
    return retry(() ->
        mDelegate.setObjectLockConfiguration(setObjectLockConfigurationRequest));
  }

  @Override
  public GetObjectLockConfigurationResult getObjectLockConfiguration(
      GetObjectLockConfigurationRequest getObjectLockConfigurationRequest) {
    return retry(() ->
        mDelegate.getObjectLockConfiguration(getObjectLockConfigurationRequest));
  }

  @Override
  public SetObjectRetentionResult setObjectRetention(
      SetObjectRetentionRequest setObjectRetentionRequest) {
    return retry(() ->
        mDelegate.setObjectRetention(setObjectRetentionRequest));
  }

  @Override
  public GetObjectRetentionResult getObjectRetention(
      GetObjectRetentionRequest getObjectRetentionRequest) {
    return retry(() ->
        mDelegate.getObjectRetention(getObjectRetentionRequest));
  }

  @Override
  public PresignedUrlDownloadResult download(
      PresignedUrlDownloadRequest presignedUrlDownloadRequest) {
    return retry(() ->
        mDelegate.download(presignedUrlDownloadRequest));
  }

  @Override
  public void download(PresignedUrlDownloadRequest presignedUrlDownloadRequest,
                       File destinationFile) {
    retry(() -> {
      mDelegate.download(presignedUrlDownloadRequest, destinationFile);
      return null;
    });
  }

  @Override
  public PresignedUrlUploadResult upload(PresignedUrlUploadRequest presignedUrlUploadRequest) {
    return retry(() ->
        mDelegate.upload(presignedUrlUploadRequest));
  }

  @Override
  public void shutdown() {
    retry(() -> {
      mDelegate.shutdown();
      return null;
    });
  }

  @Override
  public com.amazonaws.services.s3.model.Region getRegion() {
    return retry(mDelegate::getRegion);
  }

  @Override
  public String getRegionName() {
    return retry(mDelegate::getRegionName);
  }

  @Override
  public URL getUrl(String bucketName, String key) {
    return retry(() ->
        mDelegate.getUrl(bucketName, key));
  }

  @Override
  public AmazonS3Waiters waiters() {
    return retry(mDelegate::waiters);
  }
}
