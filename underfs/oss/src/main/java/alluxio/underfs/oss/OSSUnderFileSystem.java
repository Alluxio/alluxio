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
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
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
 * Aliyun OSS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class OSSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(OSSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Aliyun OSS client. */
  private final OSS mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

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
    Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ACCESS_KEY),
        "Property %s is required to connect to OSS", PropertyKey.OSS_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_SECRET_KEY),
        "Property %s is required to connect to OSS", PropertyKey.OSS_SECRET_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ENDPOINT_KEY),
        "Property %s is required to connect to OSS", PropertyKey.OSS_ENDPOINT_KEY);
    String accessId = conf.get(PropertyKey.OSS_ACCESS_KEY);
    String accessKey = conf.get(PropertyKey.OSS_SECRET_KEY);
    String endPoint = conf.get(PropertyKey.OSS_ENDPOINT_KEY);

    ClientBuilderConfiguration ossClientConf = initializeOSSClientConfig(conf);
    OSS ossClient = new OSSClientBuilder().build(endPoint, accessId, accessKey, ossClientConf);

    return new OSSUnderFileSystem(uri, ossClient, bucketName, conf);
  }

  /**
   * Constructor for {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ossClient Aliyun OSS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected OSSUnderFileSystem(AlluxioURI uri, OSS ossClient, String bucketName,
      UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = ossClient;
    mBucketName = bucketName;
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
    return new OSSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS, ","));
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
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          meta.getLastModified().getTime());
    } catch (ServiceException e) {
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
    return Constants.HEADER_OSS + mBucketName;
  }

  /**
   * Creates an OSS {@code ClientConfiguration} using an Alluxio Configuration.
   *
   * @return the OSS {@link ClientBuilderConfiguration}
   */
  private static ClientBuilderConfiguration initializeOSSClientConfig(
      AlluxioConfiguration alluxioConf) {
    ClientBuilderConfiguration ossClientConf = new ClientBuilderConfiguration();
    ossClientConf
        .setConnectionTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setConnectionTTL(alluxioConf.getLong(PropertyKey.UNDERFS_OSS_CONNECT_TTL));
    ossClientConf.setMaxConnections(alluxioConf.getInt(PropertyKey.UNDERFS_OSS_CONNECT_MAX));
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
}
