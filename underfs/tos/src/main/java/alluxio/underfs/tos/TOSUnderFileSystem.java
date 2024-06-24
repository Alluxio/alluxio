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

package alluxio.underfs.tos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.volcengine.tos.TOSClientConfiguration;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.auth.StaticCredentials;
import com.volcengine.tos.model.object.AbortMultipartUploadInput;
import com.volcengine.tos.model.object.CopyObjectV2Input;
import com.volcengine.tos.model.object.CopyObjectV2Output;
import com.volcengine.tos.model.object.DeleteMultiObjectsV2Input;
import com.volcengine.tos.model.object.DeleteMultiObjectsV2Output;
import com.volcengine.tos.model.object.DeleteObjectInput;
import com.volcengine.tos.model.object.DeleteObjectOutput;
import com.volcengine.tos.model.object.Deleted;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.HeadObjectV2Output;
import com.volcengine.tos.model.object.ListMultipartUploadsV2Input;
import com.volcengine.tos.model.object.ListMultipartUploadsV2Output;
import com.volcengine.tos.model.object.ListObjectsType2Input;
import com.volcengine.tos.model.object.ListObjectsType2Output;
import com.volcengine.tos.model.object.ListedCommonPrefix;
import com.volcengine.tos.model.object.ListedObjectV2;
import com.volcengine.tos.model.object.ListedUpload;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import com.volcengine.tos.model.object.ObjectTobeDeleted;
import com.volcengine.tos.model.object.PutObjectInput;
import com.volcengine.tos.transport.TransportConfig;
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * TOS（Tinder Object Storage）{@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class TOSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(TOSUnderFileSystem.class);

  /**
   * Suffix for an empty file to flag it as a directory.
   */
  private static final String FOLDER_SUFFIX = "/";

  /**
   * TOS client.
   */
  private final TOSV2 mClient;

  /**
   * Bucket name of user's configured Alluxio bucket.
   */
  private final String mBucketName;

  private final Supplier<ListeningExecutorService> mStreamingUploadExecutor;

  /**
   * Constructs a new instance of {@link TOSUnderFileSystem}.
   *
   * @param uri  the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link TOSUnderFileSystem} instance
   */
  public static TOSUnderFileSystem createInstance(AlluxioURI uri,
                                                  UnderFileSystemConfiguration conf) {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_ACCESS_KEY),
        "Property %s is required to connect to TOS", PropertyKey.TOS_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_SECRET_KEY),
        "Property %s is required to connect to TOS", PropertyKey.TOS_SECRET_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_REGION),
        "Property %s is required to connect to TOS", PropertyKey.TOS_REGION);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_ENDPOINT_KEY),
        "Property %s is required to connect to TOS", PropertyKey.TOS_ENDPOINT_KEY);
    String accessKey = conf.getString(PropertyKey.TOS_ACCESS_KEY);
    String secretKey = conf.getString(PropertyKey.TOS_SECRET_KEY);
    String regionName = conf.getString(PropertyKey.TOS_REGION);
    String endPoint = conf.getString(PropertyKey.TOS_ENDPOINT_KEY);
    TOSClientConfiguration configuration = TOSClientConfiguration.builder()
        .transportConfig(initializeTOSClientConfig(conf))
        .region(regionName)
        .endpoint(endPoint)
        .credentials(new StaticCredentials(accessKey, secretKey))
        .build();
    TOSV2 tos = new TOSV2ClientBuilder().build(configuration);
    return new TOSUnderFileSystem(uri, tos, bucketName, conf);
  }

  /**
   * Constructor for {@link TOSUnderFileSystem}.
   *
   * @param uri        the {@link AlluxioURI} for this UFS
   * @param tosClient  TOS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf       configuration for this UFS
   */
  protected TOSUnderFileSystem(AlluxioURI uri, @Nullable TOSV2 tosClient, String bucketName,
                               UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = tosClient;
    mBucketName = bucketName;
    mStreamingUploadExecutor = Suppliers.memoize(() -> {
      int numTransferThreads =
          conf.getInt(PropertyKey.UNDERFS_TOS_STREAMING_UPLOAD_THREADS);
      ExecutorService service = ExecutorServiceFactories
          .fixedThreadPool("alluxio-tos-streaming-upload-worker",
              numTransferThreads).create();
      return MoreExecutors.listeningDecorator(service);
    });
  }

  @Override
  public String getUnderFSType() {
    return "tos";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {
  }

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {
  }

  @Override
  public void cleanup() {
    long cleanAge = mUfsConf.getMs(PropertyKey.UNDERFS_TOS_INTERMEDIATE_UPLOAD_CLEAN_AGE);
    Date cleanBefore = new Date(new Date().getTime() - cleanAge);
    boolean isTruncated = true;
    String keyMarker = null;
    String uploadIdMarker = null;
    int maxKeys = 10;
    try {
      while (isTruncated) {
        ListMultipartUploadsV2Input input = new ListMultipartUploadsV2Input().setBucket(mBucketName)
            .setMaxUploads(maxKeys).setKeyMarker(keyMarker).setUploadIDMarker(uploadIdMarker);
        ListMultipartUploadsV2Output output = mClient.listMultipartUploads(input);
        if (output.getUploads() != null) {
          for (int i = 0; i < output.getUploads().size(); ++i) {
            ListedUpload upload = output.getUploads().get(i);
            if (upload.getInitiated().before(cleanBefore)) {
              mClient.abortMultipartUpload(new AbortMultipartUploadInput().setBucket(mBucketName)
                  .setKey(upload.getKey()).setUploadID(upload.getUploadID()));
            }
          }
        }
        isTruncated = output.isTruncated();
        keyMarker = output.getNextKeyMarker();
        uploadIdMarker = output.getNextUploadIdMarker();
      }
    } catch (TosException e) {
      LOG.error("Failed to cleanup TOS uploads", e);
      throw AlluxioTosException.from(e);
    }
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    try {
      CopyObjectV2Input input =
          new CopyObjectV2Input().setBucket(mBucketName).setKey(dst).setSrcBucket(mBucketName)
              .setSrcKey(src);
      CopyObjectV2Output output = mClient.copyObject(input);
      return true;
    } catch (TosException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetaRequestOptions metaRequestOptions = new ObjectMetaRequestOptions();
      metaRequestOptions.setContentLength(0);
      ByteArrayInputStream stream = new ByteArrayInputStream(new byte[0]);
      PutObjectInput input =
          new PutObjectInput().setBucket(mBucketName).setKey(key).setOptions(metaRequestOptions)
              .setContent(stream);
      mClient.putObject(input);
      return true;
    } catch (TosException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    if (mUfsConf.getBoolean(PropertyKey.UNDERFS_TOS_STREAMING_UPLOAD_ENABLED)) {
      return new TOSLowLevelOutputStream(mBucketName, key, mClient,
          mStreamingUploadExecutor.get(), mUfsConf);
    }
    return new TOSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      DeleteObjectInput input = new DeleteObjectInput().setBucket(mBucketName).setKey(key);
      DeleteObjectOutput output = mClient.deleteObject(input);
    } catch (TosException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) {
    try {
      List<ObjectTobeDeleted> list = new ArrayList<>();
      for (String key : keys) {
        list.add(new ObjectTobeDeleted().setKey(key));
      }
      DeleteMultiObjectsV2Input input =
          new DeleteMultiObjectsV2Input().setBucket(mBucketName).setObjects(list);
      DeleteMultiObjectsV2Output output = mClient.deleteMultiObjects(input);
      return output.getDeleteds().stream().map(Deleted::getKey).collect(Collectors.toList());
    } catch (TosException e) {
      LOG.error("Failed to delete objects", e);
      throw AlluxioTosException.from(e);
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
    ListObjectsType2Input input =
        new ListObjectsType2Input().setBucket(mBucketName).setDelimiter(delimiter).setPrefix(key);
    ListObjectsType2Output output = getObjectListingChunk(input);
    if (output != null) {
      return new TOSObjectListingChunk(input, output);
    }
    return null;
  }

  // Get next chunk of listing result
  private ListObjectsType2Output getObjectListingChunk(ListObjectsType2Input input) {
    ListObjectsType2Output result;
    try {
      result = mClient.listObjectsType2(input);
    } catch (TosException e) {
      LOG.error("Failed to list path {}", input.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over TOS {@link ObjectListingChunk}.
   */
  private final class TOSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsType2Input mInput;
    final ListObjectsType2Output mOutput;

    TOSObjectListingChunk(ListObjectsType2Input Input, ListObjectsType2Output Output)
        throws IOException {
      mInput = Input;
      mOutput = Output;
      if (mOutput == null) {
        throw new IOException("TOS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<ListedObjectV2> objects = mOutput.getContents();
      if (objects == null) {
        return new ObjectStatus[0];
      }
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (ListedObjectV2 obj : objects) {
        Date lastModifiedDate = obj.getLastModified();
        Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getEtag(), obj.getSize(),
            lastModifiedTime);
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<ListedCommonPrefix> res = mOutput.getCommonPrefixes();
      if (res == null) {
        return new String[0];
      }
      return res.stream().map(ListedCommonPrefix::getPrefix).toArray(String[]::new);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mOutput.isTruncated()) {
        mInput.setContinuationToken(mOutput.getNextContinuationToken());
        ListObjectsType2Output nextResult = mClient.listObjectsType2(mInput);
        if (nextResult != null) {
          return new TOSObjectListingChunk(mInput, nextResult);
        }
      }
      return null;
    }

    @Override
    public Boolean hasNextChunk() {
      return mOutput.isTruncated();
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      HeadObjectV2Input input = new HeadObjectV2Input().setBucket(mBucketName).setKey(key);
      HeadObjectV2Output output = mClient.headObject(input);
      if (output == null) {
        return null;
      }
      Date lastModifiedDate = output.getLastModifiedInDate();
      Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
      return new ObjectStatus(key, output.getEtag(), output.getContentLength(),
          lastModifiedTime);
    } catch (TosServerException e) {
      if (e.getStatusCode() == 404) { // file not found, possible for exists calls
        return null;
      }
      throw AlluxioTosException.from(e);
    } catch (TosClientException e) {
      LOG.error("Failed to get object status for {}", key, e);
      throw AlluxioTosException.from(e);
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_TOS + mBucketName;
  }

  /**
   * Creates an TOS {@code ClientConfiguration} using an Alluxio Configuration.
   * @param alluxioConf the TOS Configuration
   * @return the TOS {@link TransportConfig}
   */
  public static TransportConfig initializeTOSClientConfig(AlluxioConfiguration alluxioConf) {
    int readTimeoutMills = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_READ_TIMEOUT);
    int writeTimeoutMills = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_WRITE_TIMEOUT);
    int connectionTimeoutMills = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_CONNECT_TIMEOUT);
    int maxConnections = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_CONNECT_MAX);
    int idleConnectionTime = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_CONNECT_TTL);
    int maxErrorRetry = alluxioConf.getInt(PropertyKey.UNDERFS_TOS_RETRY_MAX);
    TransportConfig config = TransportConfig.builder()
        .connectTimeoutMills(connectionTimeoutMills)
        .maxConnections(maxConnections)
        .maxRetryCount(maxErrorRetry)
        .readTimeoutMills(readTimeoutMills)
        .writeTimeoutMills(writeTimeoutMills)
        .idleConnectionTimeMills(idleConnectionTime)
        .build();
    return config;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    try {
      return new TOSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy,
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (TosException e) {
      LOG.error("Failed to open object: {}", key, e);
      throw AlluxioTosException.from(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    mClient.close();
  }
}
