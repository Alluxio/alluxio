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

package alluxio.underfs.cos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.region.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tencent Cloud COS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class COSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(COSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "/";

  /** Aliyun COS client. */
  private final COSClient mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketNameInternal;

  /**
   * Constructs a new instance of {@link COSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link COSUnderFileSystem} instance
   */
  public static COSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
      throws Exception {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COS_ACCESS_KEY),
        "Property %s is required to connect to COS", PropertyKey.COS_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COS_SECRET_KEY),
        "Property %s is required to connect to COS", PropertyKey.COS_SECRET_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COS_REGION),
        "Property %s is required to connect to COS", PropertyKey.COS_REGION);
    Preconditions.checkArgument(conf.isSet(PropertyKey.COS_APP_ID),
        "Property %s is required to connect to COS", PropertyKey.COS_APP_ID);
    String accessKey = conf.get(PropertyKey.COS_ACCESS_KEY);
    String secretKey = conf.get(PropertyKey.COS_SECRET_KEY);
    String regionName = conf.get(PropertyKey.COS_REGION);
    String appId = conf.get(PropertyKey.COS_APP_ID);

    COSCredentials cred = new BasicCOSCredentials(accessKey, secretKey);
    ClientConfig clientConfig = createCOSClientConfig(regionName, conf);
    COSClient client = new COSClient(cred, clientConfig);

    return new COSUnderFileSystem(uri, client, bucketName, appId, conf);
  }

  /**
   * Constructor for {@link COSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param client Aliyun COS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected COSUnderFileSystem(AlluxioURI uri, COSClient client, String bucketName, String appId,
      UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = client;
    mBucketName = bucketName;
    mBucketNameInternal = bucketName + "-" + appId;
  }

  @Override
  public String getUnderFSType() {
    return "cos";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) {}

  @Override
  protected boolean copyObject(String src, String dst) {
    try {
      LOG.debug("Copying {} to {}", src, dst);
      mClient.copyObject(mBucketNameInternal, src, mBucketNameInternal, dst);
      return true;
    } catch (CosClientException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      mClient.putObject(mBucketNameInternal, key, new ByteArrayInputStream(new byte[0]), objMeta);
      return true;
    } catch (CosClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new COSOutputStream(mBucketNameInternal, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS, ","));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(mBucketNameInternal, key);
    } catch (CosClientException e) {
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
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(mBucketNameInternal);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength(mUfsConf));
    request.setDelimiter(delimiter);

    ObjectListing result = getObjectListingChunk(request);
    if (result != null) {
      return new COSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (CosClientException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over COS {@link ObjectListingChunk}.
   */
  private final class COSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    COSObjectListingChunk(ListObjectsRequest request, ObjectListing result) throws IOException {
      Preconditions.checkNotNull(result, "result");
      mRequest = request;
      mResult = result;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<COSObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (COSObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            obj.getLastModified().getTime());
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
        mRequest.setMarker(mResult.getNextMarker());
        ObjectListing nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new COSObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    // Root is always a folder
    if (isRoot(path) || path.equals(PATH_SEPARATOR)) {
      return true;
    }
    String keyAsFolder = convertToFolderName(stripPrefixIfPresent(path));
    if (getObjectStatus(keyAsFolder) != null) {
      return true;
    }
    return getObjectListingChunkForPath(path, true) != null;
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketNameInternal, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          meta.getLastModified().getTime());
    } catch (CosClientException e) {
      return null;
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_COS + mBucketName;
  }

  /**
   * Creates an COS {@code ClientConfiguration} using an Alluxio Configuration.
   *
   * @return the COS {@link ClientConfig}
   */
  private static ClientConfig createCOSClientConfig(String regionName,
      UnderFileSystemConfiguration conf) {
    ClientConfig config = new ClientConfig(new Region(regionName));
    config.setConnectionTimeout((int) conf.getMs(PropertyKey.COS_CONNECTION_TIMEOUT));
    config.setSocketTimeout((int) conf.getMs(PropertyKey.COS_SOCKET_TIMEOUT));
    config.setMaxConnectionsCount(conf.getInt(PropertyKey.COS_CONNECTION_MAX));
    return config;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options,
      RetryPolicy retryPolicy) throws IOException {
    try {
      return new COSInputStream(mBucketNameInternal, key, mClient, options.getOffset(), retryPolicy,
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (CosClientException e) {
      throw new IOException(e.getMessage());
    }
  }
}
