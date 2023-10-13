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

package alluxio.underfs.oss;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.BucketInfo;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListMultipartUploadsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.MultipartUpload;
import com.aliyun.oss.model.MultipartUploadListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.Owner;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Aliyun OSS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class OSSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(OSSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Default owner of objects if owner cannot be determined. */
  private static final String DEFAULT_OWNER = "";

  /** Aliyun OSS client. */
  private final OSS mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  private final Supplier<ListeningExecutorService> mStreamingUploadExecutor;

  private StsOssClientProvider mClientProvider;

  /** The permissions associated with the bucket. Fetched once and assumed to be immutable. */
  private final Supplier<ObjectPermissions> mPermissions
      = CommonUtils.memoize(this::getPermissionsInternal);

  /**
   * Constructs a new instance of {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link OSSUnderFileSystem} instance
   */
  public static OSSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
      throws Exception {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    return new OSSUnderFileSystem(uri, null, bucketName, conf);
  }

  /**
   * Constructor for {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ossClient Aliyun OSS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected OSSUnderFileSystem(AlluxioURI uri, @Nullable OSS ossClient, String bucketName,
                               UnderFileSystemConfiguration conf) {
    super(uri, conf);

    if (conf.getBoolean(PropertyKey.UNDERFS_OSS_STS_ENABLED)) {
      try {
        mClientProvider = new StsOssClientProvider(conf);
        mClientProvider.init();
        mClient = mClientProvider.getOSSClient();
      } catch (IOException e) {
        LOG.error("init sts client provider failed!", e);
        throw new ServiceException(e);
      }
    } else if (null != ossClient) {
      mClient = ossClient;
    } else {
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ACCESS_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_ACCESS_KEY);
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_SECRET_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_SECRET_KEY);
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ENDPOINT_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_ENDPOINT_KEY);
      String accessId = conf.getString(PropertyKey.OSS_ACCESS_KEY);
      String accessKey = conf.getString(PropertyKey.OSS_SECRET_KEY);
      String endPoint = conf.getString(PropertyKey.OSS_ENDPOINT_KEY);

      ClientBuilderConfiguration ossClientConf = initializeOSSClientConfig(conf);
      mClient = new OSSClientBuilder().build(endPoint, accessId, accessKey, ossClientConf);
    }

    mBucketName = bucketName;
    mStreamingUploadExecutor = Suppliers.memoize(() -> {
      int numTransferThreads =
          conf.getInt(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_THREADS);
      ExecutorService service = ExecutorServiceFactories
          .fixedThreadPool("alluxio-oss-streaming-upload-worker",
              numTransferThreads).create();
      return MoreExecutors.listeningDecorator(service);
    });
  }

  @Override
  public void cleanup() throws IOException {
    long cleanAge = mUfsConf.getMs(PropertyKey.UNDERFS_OSS_INTERMEDIATE_UPLOAD_CLEAN_AGE);
    Date cleanBefore = new Date(new Date().getTime() - cleanAge);
    MultipartUploadListing uploadListing = mClient.listMultipartUploads(
        new ListMultipartUploadsRequest(mBucketName));
    do {
      for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
        if (upload.getInitiated().compareTo(cleanBefore) < 0) {
          mClient.abortMultipartUpload(new AbortMultipartUploadRequest(
              mBucketName, upload.getKey(), upload.getUploadId()));
        }
      }
      ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(mBucketName);
      request.setUploadIdMarker(uploadListing.getNextUploadIdMarker());
      request.setKeyMarker(uploadListing.getKeyMarker());
      uploadListing = mClient.listMultipartUploads(request);
    } while (uploadListing.isTruncated());
  }

  @Override
  public String getUnderFSType() {
    return "oss";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    try {
      mClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      mClient.putObject(mBucketName, key, new ByteArrayInputStream(new byte[0]), objMeta);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    if (mUfsConf.getBoolean(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_ENABLED)) {
      return new OSSLowLevelOutputStream(mBucketName, key, mClient,
          mStreamingUploadExecutor.get(), mUfsConf);
    }
    return new OSSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (ServiceException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) throws IOException {
    try {
      DeleteObjectsRequest request = new DeleteObjectsRequest(mBucketName);
      request.setKeys(keys);
      DeleteObjectsResult result = mClient.deleteObjects(request);
      return result.getDeletedObjects();
    } catch (ServiceException e) {
      throw new IOException("Failed to delete objects", e);
    }
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
    ListObjectsRequest request = new ListObjectsRequest(mBucketName);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength(mUfsConf));
    request.setDelimiter(delimiter);

    ObjectListing result = getObjectListingChunk(request);
    if (result != null) {
      return new OSSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over OSS {@link ObjectListingChunk}.
   */
  private final class OSSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    OSSObjectListingChunk(ListObjectsRequest request, ObjectListing result) throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("OSS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<OSSObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (OSSObjectSummary obj : objects) {
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
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        mRequest.setMarker(mResult.getNextMarker());
        ObjectListing nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new OSSObjectListingChunk(mRequest, nextResult);
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
      Date lastModifiedDate = meta.getLastModified();
      Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          lastModifiedTime);
    } catch (ServiceException e) {
      return null;
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return mPermissions.get();
  }

  /**
   * Since there is no group in OSS, the owner is reused as the group. This method calls the
   * OSS API and requires additional permissions aside from just read only. This method is best
   * effort and will continue with default permissions (no owner, no group, 0700).
   *
   * @return the permissions associated with this under storage system
   */
  private ObjectPermissions getPermissionsInternal() {
    short bucketMode =
        ModeUtils.getUMask(mUfsConf.getString(PropertyKey.UNDERFS_OSS_DEFAULT_MODE)).toShort();
    String accountOwner = DEFAULT_OWNER;

    try {
      BucketInfo bucketInfo = mClient.getBucketInfo(mBucketName);
      Owner owner = bucketInfo.getBucket().getOwner();
      if (mUfsConf.isSet(PropertyKey.UNDERFS_OSS_OWNER_ID_TO_USERNAME_MAPPING)) {
        // Here accountOwner can be null if there is no mapping set for this owner id
        accountOwner = CommonUtils.getValueFromStaticMapping(
            mUfsConf.getString(PropertyKey.UNDERFS_OSS_OWNER_ID_TO_USERNAME_MAPPING),
            owner.getId());
      }
      if (accountOwner == null || accountOwner.equals(DEFAULT_OWNER)) {
        // If there is no user-defined mapping, use display name or id.
        accountOwner = owner.getDisplayName() != null ? owner.getDisplayName() : owner.getId();
      }
    } catch (ServiceException e) {
      LOG.warn("Failed to get bucket owner, proceeding with defaults. {}", e.toString());
    }
    return new ObjectPermissions(accountOwner, accountOwner, bucketMode);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_OSS + mBucketName;
  }

  /**
   * Creates an OSS {@code ClientConfiguration} using an Alluxio Configuration.
   * @param alluxioConf the OSS Configuration
   * @return the OSS {@link ClientBuilderConfiguration}
   */
  public static ClientBuilderConfiguration initializeOSSClientConfig(
      AlluxioConfiguration alluxioConf) {
    ClientBuilderConfiguration ossClientConf = new ClientBuilderConfiguration();
    ossClientConf
        .setConnectionTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setConnectionTTL(alluxioConf.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TTL));
    ossClientConf.setMaxConnections(alluxioConf.getInt(PropertyKey.UNDERFS_OSS_CONNECT_MAX));
    ossClientConf.setMaxErrorRetry(alluxioConf.getInt(PropertyKey.UNDERFS_OSS_RETRY_MAX));
    return ossClientConf;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    try {
      return new OSSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy,
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    mClientProvider.close();
  }
}
