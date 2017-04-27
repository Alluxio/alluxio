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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.util.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;

/**
 * S3 {@link UnderFileSystem} implementation based on the aws-java-sdk-s3 library.
 */
@ThreadSafe
public class S3AUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(S3AUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** Threshold to do multipart copy. */
  private static final long MULTIPART_COPY_THRESHOLD = 100 * Constants.MB;

  /** AWS-SDK S3 client. */
  private final AmazonS3Client mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Transfer Manager for efficient I/O to S3. */
  private final TransferManager mManager;

  /** The name of the account owner. */
  private final String mAccountOwner;

  /** The permission mode that the account owner has to the bucket. */
  private final short mBucketMode;

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * Constructs a new instance of {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @return the created {@link S3AUnderFileSystem} instance
   */
  public static S3AUnderFileSystem createInstance(AlluxioURI uri) {

    String bucketName = UnderFileSystemUtils.getBucketName(uri);

    // Set the aws credential system properties based on Alluxio properties, if they are set
    if (Configuration.containsKey(PropertyKey.S3A_ACCESS_KEY)) {
      System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY,
          Configuration.get(PropertyKey.S3A_ACCESS_KEY));
    }
    if (Configuration.containsKey(PropertyKey.S3A_SECRET_KEY)) {
      System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY,
          Configuration.get(PropertyKey.S3A_SECRET_KEY));
    }

    // Checks, in order, env variables, system properties, profile file, and instance profile
    AWSCredentialsProvider credentials =
        new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain());

    // Set the client configuration based on Alluxio configuration values
    ClientConfiguration clientConf = new ClientConfiguration();

    // Socket timeout
    clientConf.setSocketTimeout(Configuration.getInt(PropertyKey.UNDERFS_S3A_SOCKET_TIMEOUT_MS));

    // HTTP protocol
    if (Configuration.getBoolean(PropertyKey.UNDERFS_S3A_SECURE_HTTP_ENABLED)) {
      clientConf.setProtocol(Protocol.HTTPS);
    } else {
      clientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      clientConf.setProxyHost(Configuration.get(PropertyKey.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
      clientConf.setProxyPort(Configuration.getInt(PropertyKey.UNDERFS_S3_PROXY_PORT));
    }

    int numAdminThreads = Configuration.getInt(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX);
    int numTransferThreads = Configuration.getInt(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX);
    int numThreads = Configuration.getInt(PropertyKey.UNDERFS_S3_THREADS_MAX);
    if (numThreads < numAdminThreads + numTransferThreads) {
      LOG.warn("Configured s3 max threads: {} is less than # admin threads: {} plus transfer "
          + "threads {}. Using admin threads + transfer threads as max threads instead.");
      numThreads = numAdminThreads + numTransferThreads;
    }
    clientConf.setMaxConnections(numThreads);

    // Set client request timeout for all requests since multipart copy is used, and copy parts can
    // only be set with the client configuration.
    clientConf.setRequestTimeout(Configuration.getInt(PropertyKey.UNDERFS_S3A_REQUEST_TIMEOUT));

    if (Configuration.containsKey(PropertyKey.UNDERFS_S3A_SIGNER_ALGORITHM)) {
      clientConf.setSignerOverride(Configuration.get(PropertyKey.UNDERFS_S3A_SIGNER_ALGORITHM));
    }

    AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, clientConf);
    // Set a custom endpoint.
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      amazonS3Client.setEndpoint(Configuration.get(PropertyKey.UNDERFS_S3_ENDPOINT));
    }
    // Disable DNS style buckets, this enables path style requests.
    if (Configuration.getBoolean(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS)) {
      S3ClientOptions clientOptions = S3ClientOptions.builder().setPathStyleAccess(true).build();
      amazonS3Client.setS3ClientOptions(clientOptions);
    }

    ExecutorService service = ExecutorServiceFactories.fixedThreadPoolExecutorServiceFactory(
        "alluxio-s3-transfer-manager-worker", numTransferThreads).create();

    TransferManager transferManager = new TransferManager(amazonS3Client, service);

    TransferManagerConfiguration transferConf = new TransferManagerConfiguration();
    transferConf.setMultipartCopyThreshold(MULTIPART_COPY_THRESHOLD);
    transferManager.setConfiguration(transferConf);

     // Default to readable and writable by the user.
    short bucketMode = (short) 700;
    String accountOwner = ""; // There is no known account owner by default.
    // if ACL enabled inherit bucket acl for all the objects.
    if (Configuration.getBoolean(PropertyKey.UNDERFS_S3A_INHERIT_ACL)) {
      String accountOwnerId = amazonS3Client.getS3AccountOwner().getId();
      // Gets the owner from user-defined static mapping from S3 canonical user
      // id to Alluxio user name.
      String owner = CommonUtils.getValueFromStaticMapping(
          Configuration.get(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING),
          accountOwnerId);
      // If there is no user-defined mapping, use the display name.
      if (owner == null) {
        owner = amazonS3Client.getS3AccountOwner().getDisplayName();
      }
      accountOwner = owner == null ? accountOwnerId : owner;

      AccessControlList acl = amazonS3Client.getBucketAcl(bucketName);
      bucketMode = S3AUtils.translateBucketAcl(acl, accountOwnerId);
    }
    return new S3AUnderFileSystem(uri, amazonS3Client, bucketName, bucketMode, accountOwner,
        transferManager);
  }

  /**
   * Constructor for {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param amazonS3Client AWS-SDK S3 client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketMode the permission mode that the account owner has to the bucket
   * @param accountOwner the name of the account owner
   * @param transferManager Transfer Manager for efficient I/O to S3
   */
  protected S3AUnderFileSystem(AlluxioURI uri,
      AmazonS3Client amazonS3Client,
      String bucketName,
      short bucketMode,
      String accountOwner,
      TransferManager transferManager) {
    super(uri);
    mClient = amazonS3Client;
    mBucketName = bucketName;
    mBucketMode = bucketMode;
    mAccountOwner = accountOwner;
    mManager = transferManager;
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

  // Returns the account owner.
  @Override
  public String getOwner(String path) throws IOException {
    return mAccountOwner;
  }

  // No group in S3 ACL, returns the account owner.
  @Override
  public String getGroup(String path) throws IOException {
    return mAccountOwner;
  }

  // Returns the account owner's permission mode to the S3 bucket.
  @Override
  public short getMode(String path) throws IOException {
    return mBucketMode;
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    // Retry copy for a few times, in case some AWS internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {
        CopyObjectRequest request = new CopyObjectRequest(mBucketName, src, mBucketName, dst);
        if (Configuration.getBoolean(PropertyKey.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED)) {
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
  protected boolean createEmptyObject(String key) {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(0);
      meta.setContentMD5(DIR_HASH);
      meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);
      mClient.putObject(new PutObjectRequest(mBucketName, key, new ByteArrayInputStream(
          new byte[0]), meta));
      return true;
    } catch (AmazonClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new S3AOutputStream(mBucketName, key, mManager);
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
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    ListObjectsV2Request request =
        new ListObjectsV2Request().withBucketName(mBucketName).withPrefix(key)
            .withDelimiter(delimiter).withMaxKeys(getListingChunkLength());
    ListObjectsV2Result result = getObjectListingChunk(request);
    if (result != null) {
      return new S3AObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ListObjectsV2Result getObjectListingChunk(ListObjectsV2Request request) {
    ListObjectsV2Result result;
    try {
      // Query S3 for the next batch of objects
      result = mClient.listObjectsV2(request);
      // Advance the request continuation token to the next set of objects
      request.setContinuationToken(result.getNextContinuationToken());
    } catch (AmazonClientException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over S3 {@link ListObjectsV2Request}.
   */
  private final class S3AObjectListingChunk implements ObjectListingChunk {
    final ListObjectsV2Request mRequest;
    final ListObjectsV2Result mResult;

    S3AObjectListingChunk(ListObjectsV2Request request, ListObjectsV2Result result)
        throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("S3A listing result is null");
      }
    }

    @Override
    public String[] getObjectNames() {
      List<S3ObjectSummary> objects = mResult.getObjectSummaries();
      String[] ret = new String[objects.size()];
      int i = 0;
      for (S3ObjectSummary obj : objects) {
        ret[i++] = obj.getKey();
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[res.size()]);
    }

    @Override
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

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(meta.getContentLength(), meta.getLastModified().getTime());
    } catch (AmazonClientException e) {
      return null;
    }
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_S3A + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new S3AInputStream(mBucketName, key, mClient, options.getOffset());
    } catch (AmazonClientException e) {
      throw new IOException(e.getMessage());
    }
  }
}
