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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.Base64;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;
import javax.annotation.Nullable;

/**
 * S3 {@link UnderFileSystem} implementation based on the aws-java-sdk-s3 library.
 */
@ThreadSafe
public class S3AUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(S3AUnderFileSystem.class);

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** Threshold to do multipart copy. */
  private static final long MULTIPART_COPY_THRESHOLD = 100L * Constants.MB;

  /** Default owner of objects if owner cannot be determined. */
  private static final String DEFAULT_OWNER = "";

  /** AWS-SDK S3 client. */
  private final AmazonS3Client mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Executor for executing upload tasks in streaming upload. */
  private final ListeningExecutorService mExecutor;

  /** Transfer Manager for efficient I/O to S3. */
  private final TransferManager mManager;

  /** Whether the streaming upload is enabled. */
  private final boolean mStreamingUploadEnabled;

  /** The permissions associated with the bucket. Fetched once and assumed to be immutable. */
  private final Supplier<ObjectPermissions> mPermissions
      = CommonUtils.memoize(this::getPermissionsInternal);

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * @param conf the configuration for this UFS
   *
   * @return the created {@link AWSCredentialsProvider} instance
   */
  public static AWSCredentialsProvider createAwsCredentialsProvider(
      UnderFileSystemConfiguration conf) {
    // Set the aws credential system properties based on Alluxio properties, if they are set;
    // otherwise, use the default credential provider.
    if (conf.isSet(PropertyKey.S3A_ACCESS_KEY)
        && conf.isSet(PropertyKey.S3A_SECRET_KEY)) {
      return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
          conf.get(PropertyKey.S3A_ACCESS_KEY), conf.get(PropertyKey.S3A_SECRET_KEY)));
    }
    // Checks, in order, env variables, system properties, profile file, and instance profile.
    return new DefaultAWSCredentialsProviderChain();
  }

  /**
   * Constructs a new instance of {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link S3AUnderFileSystem} instance
   */
  public static S3AUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {

    AWSCredentialsProvider credentials = createAwsCredentialsProvider(conf);
    String bucketName = UnderFileSystemUtils.getBucketName(uri);

    // Set the client configuration based on Alluxio configuration values.
    ClientConfiguration clientConf = new ClientConfiguration();

    // Max error retry
    if (conf.isSet(PropertyKey.UNDERFS_S3_MAX_ERROR_RETRY)) {
      clientConf.setMaxErrorRetry(conf.getInt(PropertyKey.UNDERFS_S3_MAX_ERROR_RETRY));
    }
    clientConf.setConnectionTTL(conf.getMs(PropertyKey.UNDERFS_S3_CONNECT_TTL));
    // Socket timeout
    clientConf
        .setSocketTimeout((int) conf.getMs(PropertyKey.UNDERFS_S3_SOCKET_TIMEOUT));

    // HTTP protocol
    if (Boolean.parseBoolean(conf.get(PropertyKey.UNDERFS_S3_SECURE_HTTP_ENABLED))
        || Boolean.parseBoolean(conf.get(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED))) {
      clientConf.setProtocol(Protocol.HTTPS);
    } else {
      clientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      clientConf.setProxyHost(conf.get(PropertyKey.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
      clientConf.setProxyPort(Integer.parseInt(conf.get(PropertyKey.UNDERFS_S3_PROXY_PORT)));
    }

    // Number of metadata and I/O threads to S3.
    int numAdminThreads = Integer.parseInt(conf.get(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX));
    int numTransferThreads =
        Integer.parseInt(conf.get(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX));
    int numThreads = Integer.parseInt(conf.get(PropertyKey.UNDERFS_S3_THREADS_MAX));
    if (numThreads < numAdminThreads + numTransferThreads) {
      LOG.warn("Configured s3 max threads ({}) is less than # admin threads ({}) plus transfer "
          + "threads ({}). Using admin threads + transfer threads as max threads instead.",
              numThreads, numAdminThreads, numTransferThreads);
      numThreads = numAdminThreads + numTransferThreads;
    }
    clientConf.setMaxConnections(numThreads);

    // Set client request timeout for all requests since multipart copy is used,
    // and copy parts can only be set with the client configuration.
    clientConf
        .setRequestTimeout((int) conf.getMs(PropertyKey.UNDERFS_S3_REQUEST_TIMEOUT));

    boolean streamingUploadEnabled =
        conf.getBoolean(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_ENABLED);

    // Signer algorithm
    if (conf.isSet(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM)) {
      clientConf.setSignerOverride(conf.get(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM));
    }

    AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, clientConf);

    // Set a custom endpoint.
    if (conf.isSet(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      amazonS3Client.setEndpoint(conf.get(PropertyKey.UNDERFS_S3_ENDPOINT));
    }

    // Disable DNS style buckets, this enables path style requests.
    if (Boolean.parseBoolean(conf.get(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS))) {
      S3ClientOptions clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).build();
      amazonS3Client.setS3ClientOptions(clientOptions);
    }

    ExecutorService service = ExecutorServiceFactories
        .fixedThreadPool("alluxio-s3-transfer-manager-worker",
            numTransferThreads).create();

    TransferManager transferManager = TransferManagerBuilder.standard()
        .withS3Client(amazonS3Client).withExecutorFactory(() -> service)
        .withMultipartCopyThreshold(MULTIPART_COPY_THRESHOLD)
        .build();

    return new S3AUnderFileSystem(uri, amazonS3Client, bucketName,
        service, transferManager, conf, streamingUploadEnabled);
  }

  /**
   * Constructor for {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param amazonS3Client AWS-SDK S3 client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param executor the executor for executing upload tasks
   * @param transferManager Transfer Manager for efficient I/O to S3
   * @param conf configuration for this S3A ufs
   * @param streamingUploadEnabled whether streaming upload is enabled
   */
  protected S3AUnderFileSystem(AlluxioURI uri, AmazonS3Client amazonS3Client, String bucketName,
      ExecutorService executor, TransferManager transferManager, UnderFileSystemConfiguration conf,
      boolean streamingUploadEnabled) {
    super(uri, conf);
    mClient = amazonS3Client;
    mBucketName = bucketName;
    mExecutor = MoreExecutors.listeningDecorator(executor);
    mManager = transferManager;
    mStreamingUploadEnabled = streamingUploadEnabled;
  }

  @Override
  public String getUnderFSType() {
    return "s3";
  }

  // Setting S3 owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting S3 mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  public void cleanup() {
    long cleanAge = mUfsConf.isSet(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE)
        ? mUfsConf.getMs(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE)
        : FormatUtils.parseTimeSize(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE
        .getDefaultValue());
    Date cleanBefore = new Date(new Date().getTime() - cleanAge);
    mManager.abortMultipartUploads(mBucketName, cleanBefore);
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    // Retry copy for a few times, in case some AWS internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {
        CopyObjectRequest request = new CopyObjectRequest(mBucketName, src, mBucketName, dst);
        if (Boolean.parseBoolean(
            mUfsConf.get(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED))) {
          ObjectMetadata meta = new ObjectMetadata();
          meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
          request.setNewObjectMetadata(meta);
        }
        mManager.copy(request).waitForCopyResult();
        return true;
      } catch (AmazonClientException | InterruptedException e) {
        LOG.error("Failed to copy file {} to {}", src, dst, e);
        if (i != retries - 1) {
          LOG.error("Retrying copying file {} to {}", src, dst);
        }
      }
    }
    LOG.error("Failed to copy file {} to {}, after {} retries", src, dst, retries);
    return false;
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(0);
      meta.setContentMD5(DIR_HASH);
      meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);
      mClient.putObject(
          new PutObjectRequest(mBucketName, key, new ByteArrayInputStream(new byte[0]), meta));
      return true;
    } catch (AmazonClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    if (mStreamingUploadEnabled) {
      return new S3ALowLevelOutputStream(mBucketName, key, mClient, mExecutor,
          mUfsConf.getBytes(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE),
          mUfsConf.getList(PropertyKey.TMP_DIRS, ","),
          mUfsConf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED));
    }
    return new S3AOutputStream(mBucketName, key, mManager,
        mUfsConf.getList(PropertyKey.TMP_DIRS, ","),
        mUfsConf
            .getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED));
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (AmazonClientException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) throws IOException {
    if (!mUfsConf.getBoolean(PropertyKey.UNDERFS_S3_BULK_DELETE_ENABLED)) {
      return super.deleteObjects(keys);
    }
    Preconditions.checkArgument(keys != null && keys.size() <= getListingChunkLengthMax());
    try {
      List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();
      for (String key : keys) {
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
      }
      DeleteObjectsResult deletedObjectsResult =
          mClient.deleteObjects(new DeleteObjectsRequest(mBucketName).withKeys(keysToDelete));
      List<String> deletedObjects = new ArrayList<>();
      for (DeleteObjectsResult.DeletedObject deletedObject : deletedObjectsResult
          .getDeletedObjects()) {
        deletedObjects.add(deletedObject.getKey());
      }
      return deletedObjects;
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected String getFolderSuffix() {
    return mUfsConf.get(PropertyKey.UNDERFS_S3_DIRECTORY_SUFFIX);
  }

  @Override
  @Nullable
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix.
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    if (mUfsConf.isSet(PropertyKey.UNDERFS_S3_LIST_OBJECTS_V1) && mUfsConf
        .getBoolean(PropertyKey.UNDERFS_S3_LIST_OBJECTS_V1)) {
      ListObjectsRequest request =
          new ListObjectsRequest().withBucketName(mBucketName).withPrefix(key)
              .withDelimiter(delimiter).withMaxKeys(getListingChunkLength(mUfsConf));
      ObjectListing result = getObjectListingChunkV1(request);
      if (result != null) {
        return new S3AObjectListingChunkV1(request, result);
      }
    } else {
      ListObjectsV2Request request =
          new ListObjectsV2Request().withBucketName(mBucketName).withPrefix(key)
              .withDelimiter(delimiter).withMaxKeys(getListingChunkLength(mUfsConf));
      ListObjectsV2Result result = getObjectListingChunk(request);
      if (result != null) {
        return new S3AObjectListingChunk(request, result);
      }
    }
    return null;
  }

  // Get next chunk of listing result.
  private ListObjectsV2Result getObjectListingChunk(ListObjectsV2Request request)
      throws IOException {
    ListObjectsV2Result result;
    try {
      // Query S3 for the next batch of objects.
      result = mClient.listObjectsV2(request);
      // Advance the request continuation token to the next set of objects.
      request.setContinuationToken(result.getNextContinuationToken());
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
    return result;
  }

  // Get next chunk of listing result.
  private ObjectListing getObjectListingChunkV1(ListObjectsRequest request) throws IOException {
    ObjectListing result;
    try {
      // Query S3 for the next batch of objects.
      result = mClient.listObjects(request);
      // Advance the request continuation token to the next set of objects.
      request.setMarker(result.getNextMarker());
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
    return result;
  }

  /**
   * Wrapper over S3 {@link ListObjectsV2Request}.
   */
  private final class S3AObjectListingChunk implements ObjectListingChunk {
    final ListObjectsV2Request mRequest;
    final ListObjectsV2Result mResult;

    S3AObjectListingChunk(ListObjectsV2Request request, ListObjectsV2Result result) {
      Preconditions.checkNotNull(result, "result");
      mRequest = request;
      mResult = result;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<S3ObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (S3ObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[0]);
    }

    @Override
    @Nullable
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        ListObjectsV2Result nextResult = getObjectListingChunk(mRequest);
        if (nextResult != null) {
          return new S3AObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  /**
   * Wrapper over S3 {@link ListObjectsRequest}.
   */
  private final class S3AObjectListingChunkV1 implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    S3AObjectListingChunkV1(ListObjectsRequest request, ObjectListing result) {
      Preconditions.checkNotNull(result, "result");
      mRequest = request;
      mResult = result;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<S3ObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (S3ObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[0]);
    }

    @Override
    @Nullable
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        ObjectListing nextResult = getObjectListingChunkV1(mRequest);
        if (nextResult != null) {
          return new S3AObjectListingChunkV1(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  @Nullable
  protected ObjectStatus getObjectStatus(String key) throws IOException {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          meta.getLastModified().getTime());
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) { // file not found, possible for exists calls
        return null;
      }
      throw new IOException(e);
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected ObjectPermissions getPermissions() {
    return mPermissions.get();
  }

  /**
   * Since there is no group in S3 acl, the owner is reused as the group. This method calls the
   * S3 API and requires additional permissions aside from just read only. This method is best
   * effort and will continue with default permissions (no owner, no group, 0700).
   *
   * @return the permissions associated with this under storage system
   */
  private ObjectPermissions getPermissionsInternal() {
    short bucketMode =
        ModeUtils.getUMask(mUfsConf.get(PropertyKey.UNDERFS_S3_DEFAULT_MODE)).toShort();
    String accountOwner = DEFAULT_OWNER;

    // if ACL enabled try to inherit bucket acl for all the objects.
    if (Boolean.parseBoolean(mUfsConf.get(PropertyKey.UNDERFS_S3_INHERIT_ACL))) {
      try {
        Owner owner = mClient.getS3AccountOwner();
        AccessControlList acl = mClient.getBucketAcl(mBucketName);

        bucketMode = S3AUtils.translateBucketAcl(acl, owner.getId());
        if (mUfsConf.isSet(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING)) {
          // Here accountOwner can be null if there is no mapping set for this owner id
          accountOwner = CommonUtils.getValueFromStaticMapping(
              mUfsConf.get(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING), owner.getId());
        }
        if (accountOwner == null || accountOwner.equals(DEFAULT_OWNER)) {
          // If there is no user-defined mapping, use display name or id.
          accountOwner = owner.getDisplayName() != null ? owner.getDisplayName() : owner.getId();
        }
      } catch (AmazonClientException e) {
        LOG.warn("Failed to inherit bucket ACLs, proceeding with defaults. {}", e.toString());
      }
    }

    return new ObjectPermissions(accountOwner, accountOwner, bucketMode);
  }

  @Override
  protected String getRootKey() {
    if ("s3a".equals(mUri.getScheme())) {
      return Constants.HEADER_S3A + mBucketName;
    } else {
      return Constants.HEADER_S3 + mBucketName;
    }
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options,
      RetryPolicy retryPolicy) {
    return new S3AInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy);
  }
}
