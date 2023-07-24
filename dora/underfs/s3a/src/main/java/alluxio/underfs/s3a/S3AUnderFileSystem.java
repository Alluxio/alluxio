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
import alluxio.file.options.DescendantType;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.internal.ServiceUtils;
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
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.Base64;
import com.amazonaws.util.RuntimeHttpUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * S3 {@link UnderFileSystem} implementation based on the aws-java-sdk-s3 library.
 */
@ThreadSafe
public class S3AUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(S3AUnderFileSystem.class);

  /** Static hash for a directory's empty contents. */
  protected static final String DIR_HASH;

  /** Threshold to do multipart copy. */
  protected static final long MULTIPART_COPY_THRESHOLD = 100L * Constants.MB;

  /** Default owner of objects if owner cannot be determined. */
  private static final String DEFAULT_OWNER = "";

  private static final String S3_SERVICE_NAME = "s3";

  /** AWS-SDK S3 client. */
  private AmazonS3 mClient;

  private final S3AsyncClient mAsyncClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Executor for executing upload tasks in streaming upload. */
  private final ListeningExecutorService mExecutor;

  /** Transfer Manager for efficient I/O to S3. */
  private final TransferManager mManager;

  /** Whether the streaming upload is enabled. */
  private final boolean mStreamingUploadEnabled;

  /** Whether the multipart upload is enabled. */
  private final boolean mMultipartUploadEnabled;

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
          conf.getString(PropertyKey.S3A_ACCESS_KEY), conf.getString(PropertyKey.S3A_SECRET_KEY)));
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
    if (conf.getBoolean(PropertyKey.UNDERFS_S3_SECURE_HTTP_ENABLED)
        || conf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED)) {
      clientConf.setProtocol(Protocol.HTTPS);
    } else {
      clientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      clientConf.setProxyHost(conf.getString(PropertyKey.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
      clientConf.setProxyPort(conf.getInt(PropertyKey.UNDERFS_S3_PROXY_PORT));
    }

    // Number of metadata and I/O threads to S3.
    int numAdminThreads = conf.getInt(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX);
    int numTransferThreads =
        conf.getInt(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX);
    int numThreads = conf.getInt(PropertyKey.UNDERFS_S3_THREADS_MAX);
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

    boolean multipartUploadEnabled =
        conf.getBoolean(PropertyKey.UNDERFS_S3_MULTIPART_UPLOAD_ENABLED);

    // Signer algorithm
    if (conf.isSet(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM)) {
      clientConf.setSignerOverride(conf.getString(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM));
    }

    AwsClientBuilder.EndpointConfiguration endpointConfiguration
        = createEndpointConfiguration(conf, clientConf);

    AmazonS3 amazonS3Client
        = createAmazonS3(credentials, clientConf, endpointConfiguration, conf);
    S3AsyncClient asyncClient = createAmazonS3Async(conf, clientConf);

    ExecutorService service = ExecutorServiceFactories
        .fixedThreadPool("alluxio-s3-transfer-manager-worker",
            numTransferThreads).create();

    TransferManager transferManager = TransferManagerBuilder.standard()
        .withS3Client(amazonS3Client)
        .withExecutorFactory(() -> service)
        .withMultipartCopyThreshold(MULTIPART_COPY_THRESHOLD)
        .build();

    return new S3AUnderFileSystem(uri, amazonS3Client, asyncClient, bucketName,
        service, transferManager, conf, streamingUploadEnabled, multipartUploadEnabled);
  }

  /**
   * Create an async S3 client.
   * @param conf the conf
   * @param clientConf the client conf
   * @return the client
   */
  public static S3AsyncClient createAmazonS3Async(
      UnderFileSystemConfiguration conf,
      ClientConfiguration clientConf) {

    S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder();
    // need to check all the additional parameters for these
    S3Configuration.builder();
    ClientOverrideConfiguration.builder();
    Http2Configuration.builder();
    ClientAsyncConfiguration.builder();

    NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();
    AwsCredentialsProvider credentialsProvider;
    // Set the aws credential system properties based on Alluxio properties, if they are set;
    // otherwise, use the default credential provider.
    if (conf.isSet(PropertyKey.S3A_ACCESS_KEY)
        && conf.isSet(PropertyKey.S3A_SECRET_KEY)) {
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
          conf.getString(PropertyKey.S3A_ACCESS_KEY), conf.getString(PropertyKey.S3A_SECRET_KEY)));
    } else {
      // Checks, in order, env variables, system properties, profile file, and instance profile.
      credentialsProvider = DefaultCredentialsProvider.builder().build();
    }

    if (conf.getBoolean(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS)) {
      clientBuilder.forcePathStyle(true);
    }

    // Proxy host
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      ProxyConfiguration.Builder proxyBuilder = ProxyConfiguration.builder();
      proxyBuilder.host(conf.getString(PropertyKey.UNDERFS_S3_PROXY_HOST));
      // Proxy port
      if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
        proxyBuilder.port(conf.getInt(PropertyKey.UNDERFS_S3_PROXY_PORT));
      }
      httpClientBuilder.proxyConfiguration(proxyBuilder.build());
    }
    boolean regionSet = false;
    if (conf.isSet(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      String endpoint = conf.getString(PropertyKey.UNDERFS_S3_ENDPOINT);
      final URI epr = RuntimeHttpUtils.toUri(endpoint, clientConf);
      clientBuilder.endpointOverride(epr);
      if (conf.isSet(PropertyKey.UNDERFS_S3_ENDPOINT_REGION)) {
        regionSet = setRegionAsync(clientBuilder,
            conf.getString(PropertyKey.UNDERFS_S3_ENDPOINT_REGION));
      }
    } else if (conf.isSet(PropertyKey.UNDERFS_S3_REGION)) {
      regionSet = setRegionAsync(clientBuilder,
          conf.getString(PropertyKey.UNDERFS_S3_REGION));
    }

    if (!regionSet) {
      String defaultRegion = Regions.US_EAST_1.getName();
      clientBuilder.region(Region.of(defaultRegion));
      LOG.warn("Cannot find S3 endpoint or s3 region in Alluxio configuration, "
              + "set region to {} as default. S3 client v2 does not support global bucket access, "
              + "considering specify the region in alluxio config.",
          defaultRegion);
    }
    clientBuilder.httpClientBuilder(httpClientBuilder);
    clientBuilder.credentialsProvider(credentialsProvider);
    return clientBuilder.build();
  }

  private static boolean setRegionAsync(
      S3AsyncClientBuilder builder, String region) {
    try {
      builder.region(Region.of(region));
      LOG.debug("Set S3 region {} to {}", PropertyKey.UNDERFS_S3_REGION.getName(), region);
      return true;
    } catch (SdkClientException e) {
      LOG.error("S3 region {} cannot be recognized, "
              + "fall back to use global bucket access with an extra HEAD request",
          region, e);
      return false;
    }
  }

  /**
   * Create an AmazonS3 client.
   *
   * @param credentialsProvider the credential provider
   * @param clientConf the client config
   * @param endpointConfiguration the endpoint config
   * @param conf the Ufs config
   * @return the AmazonS3 client
   */
  public static AmazonS3 createAmazonS3(AWSCredentialsProvider credentialsProvider,
      ClientConfiguration clientConf,
      AwsClientBuilder.EndpointConfiguration endpointConfiguration,
      UnderFileSystemConfiguration conf) {
    AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder()
        .withCredentials(credentialsProvider)
        .withClientConfiguration(clientConf);

    if (conf.getBoolean(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS)) {
      clientBuilder.withPathStyleAccessEnabled(true);
    }

    boolean enableGlobalBucketAccess = true;
    if (endpointConfiguration != null) {
      clientBuilder.withEndpointConfiguration(endpointConfiguration);
      enableGlobalBucketAccess = false;
    } else if (conf.isSet(PropertyKey.UNDERFS_S3_REGION)) {
      try {
        String region = conf.getString(PropertyKey.UNDERFS_S3_REGION);
        clientBuilder.withRegion(region);
        enableGlobalBucketAccess = false;
        LOG.debug("Set S3 region {} to {}", PropertyKey.UNDERFS_S3_REGION.getName(), region);
      } catch (SdkClientException e) {
        LOG.error("S3 region {} cannot be recognized, "
            + "fall back to use global bucket access with an extra HEAD request",
            conf.getString(PropertyKey.UNDERFS_S3_REGION), e);
      }
    }

    if (enableGlobalBucketAccess) {
      // access bucket without region information
      // at the cost of an extra HEAD request
      clientBuilder.withForceGlobalBucketAccessEnabled(true);
      // The special S3 region which can be used to talk to any bucket
      // Region is required even if global bucket access enabled
      String defaultRegion = Regions.US_EAST_1.getName();
      clientBuilder.setRegion(defaultRegion);
      LOG.debug("Cannot find S3 endpoint or s3 region in Alluxio configuration, "
          + "set region to {} and enable global bucket access with extra overhead",
          defaultRegion);
    }
    return clientBuilder.build();
  }

  /**
   * Creates an endpoint configuration.
   *
   * @param conf the alluxio conf
   * @param clientConf the aws conf
   * @return the endpoint configuration
   */
  @Nullable
  protected static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(
      UnderFileSystemConfiguration conf, ClientConfiguration clientConf) {
    if (!conf.isSet(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      LOG.debug("No endpoint configuration generated, using default s3 endpoint");
      return null;
    }
    String endpoint = conf.getString(PropertyKey.UNDERFS_S3_ENDPOINT);
    final URI epr = RuntimeHttpUtils.toUri(endpoint, clientConf);
    LOG.debug("Creating endpoint configuration for {}", epr);

    String region;
    if (conf.isSet(PropertyKey.UNDERFS_S3_ENDPOINT_REGION)) {
      region = conf.getString(PropertyKey.UNDERFS_S3_ENDPOINT_REGION);
    } else if (ServiceUtils.isS3USStandardEndpoint(endpoint)) {
      // endpoint is standard s3 endpoint with default region, no need to set region
      LOG.debug("Standard s3 endpoint, declare region as null");
      region = null;
    } else {
      LOG.debug("Parsing region fom non-standard s3 endpoint");
      region = AwsHostNameUtils.parseRegion(
          epr.getHost(),
          S3_SERVICE_NAME);
    }
    LOG.debug("Region for endpoint {}, URI {} is determined as {}",
        endpoint, epr, region);
    return new AwsClientBuilder.EndpointConfiguration(endpoint, region);
  }

  /**
   * Constructor for {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param amazonS3Client AWS-SDK S3 client
   * @param asyncClient AWS S3 async client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param executor the executor for executing upload tasks
   * @param transferManager Transfer Manager for efficient I/O to S3
   * @param conf configuration for this S3A ufs
   * @param streamingUploadEnabled whether streaming upload is enabled
   * @param multipartUploadEnabled whether multipart upload is enabled
   */
  protected S3AUnderFileSystem(
      AlluxioURI uri, AmazonS3 amazonS3Client, S3AsyncClient asyncClient, String bucketName,
      ExecutorService executor, TransferManager transferManager, UnderFileSystemConfiguration conf,
      boolean streamingUploadEnabled, boolean multipartUploadEnabled) {
    super(uri, conf);
    mClient = amazonS3Client;
    mAsyncClient = asyncClient;
    mBucketName = bucketName;
    mExecutor = MoreExecutors.listeningDecorator(executor);
    mManager = transferManager;
    mStreamingUploadEnabled = streamingUploadEnabled;
    mMultipartUploadEnabled = multipartUploadEnabled;
  }

  @Override
  public S3APositionReader openPositionRead(String path, long fileLength) {
    return new S3APositionReader(mClient, mBucketName, stripPrefixIfPresent(path), fileLength);
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
  public void setMode(String path, short mode) {}

  @Override
  public void cleanup() {
    long cleanAge = mUfsConf.isSet(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE)
        ? mUfsConf.getMs(PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE)
        : (long) PropertyKey.UNDERFS_S3_INTERMEDIATE_UPLOAD_CLEAN_AGE
        .getDefaultValue();
    Date cleanBefore = new Date(new Date().getTime() - cleanAge);
    mManager.abortMultipartUploads(mBucketName, cleanBefore);
  }

  @Override
  public void close() {
    mExecutor.shutdown();
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    // Retry copy for a few times, in case some AWS internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {
        CopyObjectRequest request = new CopyObjectRequest(mBucketName, src, mBucketName, dst);
        if (mUfsConf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED)) {
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

  protected AmazonS3 getClient() {
    return mClient;
  }

  protected void setClient(AmazonS3 client) {
    mClient = client;
  }

  protected String getBucketName() {
    return mBucketName;
  }

  protected ListeningExecutorService getExecutor() {
    return mExecutor;
  }

  protected TransferManager getTransferManager() {
    return mManager;
  }

  protected boolean getStreamingUploadEnabled() {
    return mStreamingUploadEnabled;
  }

  protected boolean getMultipartUploadEnabled() {
    return mMultipartUploadEnabled;
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
      LOG.debug("S3AUnderFileSystem, createObject, Streaming Upload enabled");
      return new S3ALowLevelOutputStream(mBucketName, key, mClient, mExecutor, mUfsConf);
    }
    else if (mMultipartUploadEnabled) {
      LOG.debug("S3AUnderFileSystem, createObject, Multipart upload enabled");
      return new S3AMultipartUploadOutputStream(mBucketName, key, mClient, mExecutor, mUfsConf);
    }
    else {
      LOG.debug("S3AUnderFileSystem, createObject, Simple Upload enabled");
      return new S3AOutputStream(mBucketName, key, mManager,
          mUfsConf.getList(PropertyKey.TMP_DIRS),
          mUfsConf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED));
    }
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (AmazonClientException e) {
      throw AlluxioS3Exception.from(e);
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
      throw AlluxioS3Exception.from(e);
    }
  }

  @Override
  protected String getFolderSuffix() {
    return mUfsConf.getString(PropertyKey.UNDERFS_S3_DIRECTORY_SUFFIX);
  }

  @Override
  @Nullable
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    return getObjectListingChunk(key, recursive, null, 0);
  }

  @Nullable
  @Override
  protected ObjectListingChunk getObjectListingChunk(
      String key, boolean recursive, @Nullable String startAfter, int batchSize)
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
      if (startAfter != null) {
        request.setStartAfter(startAfter);
      }
      if (batchSize > 0) {
        request.setMaxKeys(batchSize);
      }
      ListObjectsV2Result result = getObjectListingChunk(request);
      if (result != null) {
        return new S3AObjectListingChunk(request, result);
      }
    }
    return null;
  }

  // Get next chunk of listing result.
  private ListObjectsV2Result getObjectListingChunk(ListObjectsV2Request request) {
    ListObjectsV2Result result;
    try {
      // Query S3 for the next batch of objects.
      result = mClient.listObjectsV2(request);
      // Advance the request continuation token to the next set of objects.
      request.setContinuationToken(result.getNextContinuationToken());
    } catch (AmazonClientException e) {
      throw AlluxioS3Exception.from(e);
    }
    return result;
  }

  // Get next chunk of listing result.
  private ObjectListing getObjectListingChunkV1(ListObjectsRequest request) {
    ObjectListing result;
    try {
      // Query S3 for the next batch of objects.
      result = mClient.listObjects(request);
      // Advance the request continuation token to the next set of objects.
      request.setMarker(result.getNextMarker());
    } catch (AmazonClientException e) {
      throw AlluxioS3Exception.from(e);
    }
    return result;
  }

  void performGetStatusAsync(
      String path, Consumer<UfsStatus> onComplete,
      Consumer<Throwable> onError) {
    String folderSuffix = getFolderSuffix();
    path = stripPrefixIfPresent(path);
    path = path.equals(folderSuffix) ? "" : path;
    if (path.isEmpty()) {
      onComplete.accept(null);
      return;
    }
    HeadObjectRequest request =
        HeadObjectRequest.builder().bucket(mBucketName).key(path).build();
    String finalPath = path;
    mAsyncClient.headObject(request).whenCompleteAsync((result, err) -> {
      if (err != null) {
        if (err.getCause() instanceof NoSuchKeyException) {
          onComplete.accept(null);
        } else {
          onError.accept(parseS3AsyncException(err));
        }
      } else {
        try {
          ObjectPermissions permissions = getPermissions();
          long bytes = mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
          Instant lastModifiedDate = result.lastModified();
          Long lastModifiedTime = lastModifiedDate == null ? null
              : lastModifiedDate.toEpochMilli();
          UfsStatus status;
          if (finalPath.endsWith(folderSuffix)) {
            status = new UfsDirectoryStatus(finalPath, permissions.getOwner(),
                permissions.getGroup(), permissions.getMode());
          } else {
            status = new UfsFileStatus(finalPath,
                result.eTag().substring(1, result.eTag().length() - 1),
                result.contentLength(), lastModifiedTime, permissions.getOwner(),
                permissions.getGroup(), permissions.getMode(), bytes);
          }
          onComplete.accept(status);
        } catch (Throwable t) {
          onError.accept(t);
        }
      }
    });
  }

  @Override
  public void performListingAsync(
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType, boolean checkStatus,
      Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    if (checkStatus) {
      Preconditions.checkState(continuationToken == null);
      performGetStatusAsync(path, status -> {
        if (status != null && (status.isFile() || descendantType == DescendantType.NONE)) {
          onComplete.accept(new UfsLoadResult(Stream.of(status), 1, null,
              null, false, status.isFile(), true));
        } else {
          finishListingAsync(status, path, null, startAfter,
              descendantType, onComplete, onError);
        }
      }, onError);
    } else {
      finishListingAsync(null, path, continuationToken, startAfter,
          descendantType, onComplete, onError);
    }
  }

  private Throwable parseS3AsyncException(Throwable e) {
    if (e instanceof CompletionException) {
      final Throwable innerErr = e.getCause();
      if (innerErr instanceof S3Exception) {
        S3Exception innerS3Err = (S3Exception) innerErr;
        if (innerS3Err.statusCode() == 307
            || (innerS3Err.awsErrorDetails().errorCode().equals("AuthorizationHeaderMalformed")
            && innerS3Err.getMessage().contains("region"))) {
          return new IOException(
              "AWS s3 v2 client does not support global region. "
                  + "Please either specify the region using alluxio.underfs.s3.region "
                  + "or in your s3 endpoint alluxio.underfs.s3.endpoint.", innerS3Err);
        }
      }
      return new IOException(e.getCause());
    }
    return e;
  }

  private void finishListingAsync(
      @Nullable UfsStatus baseStatus,
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType,
      Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
    // if descendant type is NONE then we only want to return the directory itself
    int maxKeys = descendantType == DescendantType.NONE ? 1 : getListingChunkLength(mUfsConf);
    path = stripPrefixIfPresent(path);
    String delimiter = descendantType == DescendantType.ALL ? "" : PATH_SEPARATOR;
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix.
    path = path.equals(PATH_SEPARATOR) ? "" : path;
    String s3StartAfter = null;
    if (path.equals("")) {
      s3StartAfter = startAfter;
    } else if (startAfter != null) {
      s3StartAfter = PathUtils.concatPath(path, startAfter);
    }
    software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder request =
        software.amazon.awssdk.services.s3.model.ListObjectsV2Request
            .builder().bucket(mBucketName).prefix(path).continuationToken(continuationToken)
            .startAfter(startAfter == null ? null : s3StartAfter)
            .delimiter(delimiter).maxKeys(maxKeys);
    String finalPath = path;
    mAsyncClient.listObjectsV2(request.build())
        .whenCompleteAsync((result, err) -> {
          if (err != null) {
            onError.accept(parseS3AsyncException(err));
          } else {
            try {
              AlluxioURI lastItem = null;
              String lastPrefix = result.commonPrefixes().size() == 0 ? null
                  : result.commonPrefixes().get(result.commonPrefixes().size() - 1).prefix();
              String lastResult = result.contents().size() == 0 ? null
                  : result.contents().get(result.contents().size() - 1).key();
              if (lastPrefix == null && lastResult != null) {
                lastItem = new AlluxioURI(lastResult);
              } else if (lastPrefix != null && lastResult == null) {
                lastItem = new AlluxioURI(lastPrefix);
              } else if (lastPrefix != null) { // both are non-null
                lastItem = new AlluxioURI(lastPrefix.compareTo(lastResult) > 0
                    ? lastPrefix : lastResult);
              }
              int keyCount = result.keyCount();
              Stream<UfsStatus> resultStream = resultToStream(baseStatus, result);
              if (descendantType == DescendantType.NONE) {
                Preconditions.checkState(baseStatus == null);
                // if descendant type is NONE then we only want to return the directory itself
                Optional<Stream<UfsStatus>> str = resultStream.findFirst().map(item -> {
                  if (item.isDirectory() && item.getName().equals(finalPath)) {
                    return Stream.of(item);
                  } else {
                    if (item.getName().startsWith(finalPath)) {
                      // in this case we received a file nested under the path, this can happen
                      // if there was no marker object for the directory, and it contained
                      // a nested object
                      ObjectPermissions permissions = getPermissions();
                      return Stream.of(new UfsDirectoryStatus(finalPath,
                          permissions.getOwner(), permissions.getGroup(), permissions.getMode()));
                    }
                  }
                  return Stream.empty();
                });
                resultStream = str.orElse(Stream.empty());
              }
              onComplete.accept(
                  new UfsLoadResult(resultStream,
                      keyCount,
                      result.nextContinuationToken(), lastItem,
                      descendantType != DescendantType.NONE && result.isTruncated(),
                      false, true));
            } catch (Throwable t) {
              onError.accept(t);
            }
          }
        });
  }

  private UfsStatus s3ObjToUfsStatus(
      S3Object obj, String folderSuffix, ObjectPermissions permissions, long bytes) {
    if (obj.key().endsWith(folderSuffix)) {
      return new UfsDirectoryStatus(obj.key(), permissions.getOwner(),
          permissions.getGroup(), permissions.getMode());
    } else {
      Instant lastModifiedDate = obj.lastModified();
      Long lastModifiedTime = lastModifiedDate == null ? null
          : lastModifiedDate.toEpochMilli();
      return new UfsFileStatus(obj.key(),
          obj.eTag().substring(1, obj.eTag().length() - 1), obj.size(), lastModifiedTime,
          permissions.getOwner(), permissions.getGroup(), permissions.getMode(), bytes);
    }
  }

  private UfsStatus prefixToUfsStatus(CommonPrefix prefix, ObjectPermissions permissions) {
    return new UfsDirectoryStatus(
        prefix.prefix(), permissions.getOwner(), permissions.getGroup(),
        permissions.getMode());
  }

  private Stream<UfsStatus> resultToStream(
      @Nullable UfsStatus baseStatus, ListObjectsV2Response response) {
    // Directories are either keys that end with /
    // Or common prefixes which will also end with /
    // All results contain the full path from the bucket root
    ObjectPermissions permissions = getPermissions();
    String folderSuffix = getFolderSuffix();
    long bytes = mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    Iterator<UfsStatus> prefixes = response.commonPrefixes().stream().map(
        prefix -> prefixToUfsStatus(prefix, permissions)).iterator();
    Stream<UfsStatus> itemStream = response.contents().stream().map(obj ->
        s3ObjToUfsStatus(obj, folderSuffix, permissions, bytes));
    if (baseStatus != null) {
      itemStream = Stream.concat(Stream.of(baseStatus), itemStream);
    }
    Iterator<UfsStatus> items = itemStream.iterator();
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
        IteratorUtils.collatedIterator((s1, s2) -> {
          int val = s1.getName().compareTo(s2.getName());
          if (val != 0) {
            return val;
          }
          // If they have the same name, then return the directory first
          if (s1.isDirectory() && s2.isDirectory()) {
            return 0;
          }
          return s1.isDirectory() ? -1 : 1;
        }, prefixes, items),
        Spliterator.ORDERED), false);
  }

  /**
   * Wrapper over S3 {@link ListObjectsV2Request}.
   */
  protected final class S3AObjectListingChunk implements ObjectListingChunk {
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
        Date lastModifiedDate = obj.getLastModified();
        Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            lastModifiedTime);
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

    @Override
    public Boolean hasNextChunk() {
      return mResult.isTruncated();
    }
  }

  /**
   * Wrapper over S3 {@link ListObjectsRequest}.
   */
  protected final class S3AObjectListingChunkV1 implements ObjectListingChunk {
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
        Date lastModifiedDate = obj.getLastModified();
        Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            lastModifiedTime);
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
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      Date lastModifiedDate = meta.getLastModified();
      Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(), lastModifiedTime);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) { // file not found, possible for exists calls
        return null;
      }
      throw AlluxioS3Exception.from(e);
    } catch (AmazonClientException e) {
      throw AlluxioS3Exception.from(e);
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
        ModeUtils.getUMask(mUfsConf.getString(PropertyKey.UNDERFS_S3_DEFAULT_MODE)).toShort();
    String accountOwner = DEFAULT_OWNER;

    // if ACL enabled try to inherit bucket acl for all the objects.
    if (mUfsConf.getBoolean(PropertyKey.UNDERFS_S3_INHERIT_ACL)) {
      try {
        Owner owner = mClient.getS3AccountOwner();
        AccessControlList acl = mClient.getBucketAcl(mBucketName);

        bucketMode = S3AUtils.translateBucketAcl(acl, owner.getId());
        if (mUfsConf.isSet(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING)) {
          // Here accountOwner can be null if there is no mapping set for this owner id
          accountOwner = CommonUtils.getValueFromStaticMapping(
              mUfsConf.getString(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING),
              owner.getId());
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
    return new S3AInputStream(
        mBucketName, key, mClient, options.getOffset(), retryPolicy);
  }
}
