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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
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
public final class OSSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Aliyun OSS client. */
  private final OSSClient mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /**
   * Constructs a new instance of {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @return the created {@link OSSUnderFileSystem} instance
   * @throws Exception when a connection to GCS could not be created
   */
  public static OSSUnderFileSystem createInstance(AlluxioURI uri) throws Exception {
    String bucketName = uri.getHost();
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.OSS_ACCESS_KEY),
        "Property " + PropertyKey.OSS_ACCESS_KEY + " is required to connect to OSS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.OSS_SECRET_KEY),
        "Property " + PropertyKey.OSS_SECRET_KEY + " is required to connect to OSS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.OSS_ENDPOINT_KEY),
        "Property " + PropertyKey.OSS_ENDPOINT_KEY + " is required to connect to OSS");
    String accessId = Configuration.get(PropertyKey.OSS_ACCESS_KEY);
    String accessKey = Configuration.get(PropertyKey.OSS_SECRET_KEY);
    String endPoint = Configuration.get(PropertyKey.OSS_ENDPOINT_KEY);

    ClientConfiguration ossClientConf = initializeOSSClientConfig();
    OSSClient ossClient = new OSSClient(endPoint, accessId, accessKey, ossClientConf);

    return new OSSUnderFileSystem(uri, ossClient, bucketName);
  }

  /**
   * Constructor for {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ossClient Aliyun OSS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketPrefix prefix of the bucket
   */
  protected OSSUnderFileSystem(AlluxioURI uri,
      OSSClient ossClient,
      String bucketName) {
    super(uri);
    mClient = ossClient;
    mBucketName = bucketName;
  }

  @Override
  public String getUnderFSType() {
    return "oss";
  }

  @Override
  public InputStream open(String path) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      return new OSSInputStream(mBucketName, path, mClient);
    } catch (ServiceException e) {
      LOG.error("Failed to open file: {}", path, e);
      return null;
    }
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // No ACL integration currently, returns default empty value
  @Override
  public String getOwner(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default empty value
  @Override
  public String getGroup(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default value
  @Override
  public short getMode(String path) throws IOException {
    return Constants.DEFAULT_FILE_SYSTEM_MODE;
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    try {
      LOG.info("Copying {} to {}", src, dst);
      mClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean createEmptyObject(String key) {
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
    return new OSSOutputStream(mBucketName, key, mClient);
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
  protected ObjectListingResult getObjectListing(String path, boolean recursive)
      throws IOException {
    return new GCSObjectListingResult(path, recursive);
  }

  /**
   * Wrapper over GCS {@link StorageObjectsChunk}.
   */
  final class GCSObjectListingResult implements ObjectListingResult {
    String mPath;
    ListObjectsRequest mRequest;
    ObjectListing mResult;

    public GCSObjectListingResult(String path, boolean recursive) {
      String delimiter = recursive ? "" : PATH_SEPARATOR;
      mPath = path;
      mRequest = new ListObjectsRequest(mBucketName);
      mRequest.setPrefix(path);
      mRequest.setMaxKeys(LISTING_LENGTH);
      mRequest.setDelimiter(delimiter);
    }

    @Override
    public String[] getObjectNames() {
      if (mResult == null) {
        return null;
      }
      List<OSSObjectSummary> objects = mResult.getObjectSummaries();
      String[] ret = new String[objects.size()];
      int i = 0;
      for (OSSObjectSummary obj : objects) {
        ret[i] = obj.getKey();
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      if (mResult == null) {
        return null;
      }
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[res.size()]);
    }

    @Override
    public ObjectListingResult getNextChunk() throws IOException {
      if (mResult != null && !mResult.isTruncated()) {
        return null;
      }
      try {
        mResult = mClient.listObjects(mRequest);
      } catch (ServiceException e) {
        LOG.error("Failed to list path {}", mPath, e);
        return null;
      }
      return this;
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
    } catch (ServiceException e) {
      LOG.warn("Failed to get Object {}, return null", key, e);
      return null;
    }
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_OSS + mBucketName;
  }

  /**
   * Creates an OSS {@code ClientConfiguration} using an Alluxio Configuration.
   *
   * @return the OSS {@link ClientConfiguration}
   */
  private static ClientConfiguration initializeOSSClientConfig() {
    ClientConfiguration ossClientConf = new ClientConfiguration();
    ossClientConf.setConnectionTimeout(
        Configuration.getInt(PropertyKey.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout(
        Configuration.getInt(PropertyKey.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setConnectionTTL(Configuration.getLong(PropertyKey.UNDERFS_OSS_CONNECT_TTL));
    ossClientConf.setMaxConnections(Configuration.getInt(PropertyKey.UNDERFS_OSS_CONNECT_MAX));
    return ossClientConf;
  }
}
