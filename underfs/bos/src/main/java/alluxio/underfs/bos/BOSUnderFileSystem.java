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

package alluxio.underfs.bos;

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
import alluxio.util.io.PathUtils;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Baidu Cloud BOS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class BOSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BOSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "/";

  /** Baidu Cloud BOS client. */
  private final BosClient mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /**
   * Constructs a new instance of {@link BOSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link BOSUnderFileSystem} instance
   */
  public static BOSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
      throws Exception {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_ACCESS_KEY),
        "Property %s is required to connect to BOS", PropertyKey.BOS_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_SECRET_KEY),
        "Property %s is required to connect to BOS", PropertyKey.BOS_SECRET_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.BOS_ENDPOINT_KEY),
        "Property %s is required to connect to BOS", PropertyKey.BOS_ENDPOINT_KEY);
    String accessId = conf.get(PropertyKey.BOS_ACCESS_KEY);
    String accessKey = conf.get(PropertyKey.BOS_SECRET_KEY);
    String endPoint = conf.get(PropertyKey.BOS_ENDPOINT_KEY);

    BosClientConfiguration bosClientConf = initializeBosClientConfig(accessId, accessKey, endPoint,
        conf);
    BosClient bosClient = new BosClient(bosClientConf);

    return new BOSUnderFileSystem(uri, bosClient, bucketName, conf);
  }

  /**
   * Constructor for {@link BOSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param bosClient Baidu Cloud BOS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected BOSUnderFileSystem(AlluxioURI uri, BosClient bosClient, String bucketName,
      UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = bosClient;
    mBucketName = bucketName;
  }

  @Override
  public String getUnderFSType() {
    return "bos";
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
    } catch (BceServiceException e) {
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
    } catch (BceServiceException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new BOSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS, ","));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (BceServiceException e) {
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
    ListObjectsRequest request = new ListObjectsRequest(mBucketName);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength(mUfsConf));
    request.setDelimiter(delimiter);

    ListObjectsResponse result = getObjectListingChunk(request);
    if (result != null) {
      return new BOSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ListObjectsResponse getObjectListingChunk(ListObjectsRequest request) {
    ListObjectsResponse result;
    try {
      result = mClient.listObjects(request);
    } catch (BceServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over BOS {@link ObjectListingChunk}.
   */
  private final class BOSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ListObjectsResponse mResult;

    BOSObjectListingChunk(ListObjectsRequest request, ListObjectsResponse result)
        throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("BOS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<BosObjectSummary> objects = mResult.getContents();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (BosObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      if (res != null) {
        return res.toArray(new String[res.size()]);
      }
      return null;
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        mRequest.setMarker(mResult.getNextMarker());
        ListObjectsResponse nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new BOSObjectListingChunk(mRequest, nextResult);
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
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          meta.getLastModified().getTime());
    } catch (BceServiceException e) {
      LOG.warn("Failed to get Object {}, return null", key, e);
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
    return Constants.HEADER_BOS + mBucketName;
  }

  /**
   * Creates an BOS {@code BosClientConfiguration} using an Alluxio Configuration.
   *
   * @return the BOS {@link BosClientConfiguration}
   */
  private static BosClientConfiguration initializeBosClientConfig(String accessId, String accessKey,
      String endPoint, AlluxioConfiguration alluxioConf) {
    BosClientConfiguration bosClientConf = new BosClientConfiguration();
    bosClientConf.setCredentials(new DefaultBceCredentials(accessId, accessKey));
    bosClientConf.setEndpoint(endPoint);
    bosClientConf.setMaxConnections(alluxioConf.getInt(PropertyKey.UNDERFS_BOS_CONNECT_MAX));
    bosClientConf.setConnectionTimeoutInMillis((int) alluxioConf.getMs(
        PropertyKey.UNDERFS_BOS_CONNECT_TIMEOUT));
    bosClientConf.setSocketTimeoutInMillis((int) alluxioConf.getMs(
        PropertyKey.UNDERFS_BOS_SOCKET_TIMEOUT));
    return bosClientConf;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    try {
      return new BOSInputStream(mBucketName, key, mClient, options.getOffset(),
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (BceServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
