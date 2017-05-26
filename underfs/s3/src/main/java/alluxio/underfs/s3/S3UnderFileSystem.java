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

package alluxio.underfs.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * S3 FS {@link UnderFileSystem} implementation based on the jets3t library.
 */
@ThreadSafe
public class S3UnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(S3UnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  private static final byte[] DIR_HASH;

  /** Jets3t S3 client. */
  private final S3Service mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The name of the account owner. */
  private final String mAccountOwner;

  /** The permission mode that the account owner has to the bucket. */
  private final short mBucketMode;

  static {
    try {
      DIR_HASH = MessageDigest.getInstance("MD5").digest(new byte[0]);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Constructs a new instance of {@link S3UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link S3UnderFileSystem} instance
   * @throws ServiceException when a connection to S3 could not be created
   */
  public static S3UnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) throws ServiceException {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.S3N_ACCESS_KEY),
        "Property " + PropertyKey.S3N_ACCESS_KEY + " is required to connect to S3");
    Preconditions.checkArgument(conf.containsKey(PropertyKey.S3N_SECRET_KEY),
        "Property " + PropertyKey.S3N_SECRET_KEY + " is required to connect to S3");
    AWSCredentials awsCredentials = new AWSCredentials(conf.getValue(PropertyKey.S3N_ACCESS_KEY),
        conf.getValue(PropertyKey.S3N_SECRET_KEY));

    Jets3tProperties props = new Jets3tProperties();
    if (conf.containsKey(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      props.setProperty("httpclient.proxy-autodetect", "false");
      props.setProperty("httpclient.proxy-host",
          conf.getValue(PropertyKey.UNDERFS_S3_PROXY_HOST));
      props.setProperty("httpclient.proxy-port",
          conf.getValue(PropertyKey.UNDERFS_S3_PROXY_PORT));
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY)) {
      props.setProperty("s3service.https-only",
          conf.getValue(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY));
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      props.setProperty("s3service.s3-endpoint", conf.getValue(PropertyKey.UNDERFS_S3_ENDPOINT));
      if (conf.containsKey(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY)) {
        props.setProperty("s3service.s3-endpoint-https-port",
            conf.getValue(PropertyKey.UNDERFS_S3_ENDPOINT_HTTPS_PORT));
      } else {
        props.setProperty("s3service.s3-endpoint-http-port",
            conf.getValue(PropertyKey.UNDERFS_S3_ENDPOINT_HTTP_PORT));
      }
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS)) {
      props.setProperty("s3service.disable-dns-buckets",
          conf.getValue(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS));
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX)) {
      props.setProperty("threaded-service.max-thread-count",
          conf.getValue(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX));
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX)) {
      props.setProperty("threaded-service.admin-max-thread-count",
          conf.getValue(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX));
    }
    if (conf.containsKey(PropertyKey.UNDERFS_S3_THREADS_MAX)) {
      props.setProperty("httpclient.max-connections",
          conf.getValue(PropertyKey.UNDERFS_S3_THREADS_MAX));
    }
    LOG.debug("Initializing S3 underFs with properties: {}", props.getProperties());
    RestS3Service restS3Service = new RestS3Service(awsCredentials, null, null, props);

    String accountOwnerId = restS3Service.getAccountOwner().getId();
    // Gets the owner from user-defined static mapping from S3 canonical user id to Alluxio
    // user name.
    String owner = CommonUtils.getValueFromStaticMapping(
        conf.getValue(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING), accountOwnerId);
    // If there is no user-defined mapping, use the display name.
    if (owner == null) {
      owner = restS3Service.getAccountOwner().getDisplayName();
    }
    String accountOwner = owner == null ? accountOwnerId : owner;

    AccessControlList acl = restS3Service.getBucketAcl(bucketName);
    short bucketMode = S3Utils.translateBucketAcl(acl, accountOwnerId);

    return new S3UnderFileSystem(uri, restS3Service, bucketName, bucketMode, accountOwner, conf);
  }

  /**
   * Constructor for {@link S3UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param s3Service Jets3t S3 client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketMode the permission mode that the account owner has to the bucket
   * @param accountOwner the name of the account owner
   * @param conf configuration for this S3A ufs
   */
  protected S3UnderFileSystem(AlluxioURI uri, S3Service s3Service, String bucketName,
      short bucketMode, String accountOwner, UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = s3Service;
    mBucketName = bucketName;
    mBucketMode = bucketMode;
    mAccountOwner = accountOwner;
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
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    S3Object obj = new S3Object(dst);
    // Retry copy for a few times, in case some Jets3t or AWS internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {
        mClient.copyObject(mBucketName, src, mBucketName, obj, false);
        return true;
      } catch (ServiceException e) {
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
      S3Object obj = new S3Object(key);
      obj.setDataInputStream(new ByteArrayInputStream(new byte[0]));
      obj.setContentLength(0);
      obj.setMd5Hash(DIR_HASH);
      obj.setContentType(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      mClient.putObject(mBucketName, obj);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new S3OutputStream(mBucketName, key, mClient);
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
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    StorageObjectsChunk chunk = getObjectListingChunk(key, delimiter, null);
    if (chunk != null) {
      return new S3NObjectListingChunk(chunk);
    }
    return null;
  }

  // Get next chunk of listing result
  private StorageObjectsChunk getObjectListingChunk(String key, String delimiter,
      String priorLastKey) {
    StorageObjectsChunk res;
    try {
      res = mClient.listObjectsChunked(mBucketName, key, delimiter,
          getListingChunkLength(), priorLastKey);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", key, e);
      res = null;
    }
    return res;
  }

  /**
   * Wrapper over S3 {@link StorageObjectsChunk}.
   */
  private final class S3NObjectListingChunk implements ObjectListingChunk {
    final StorageObjectsChunk mChunk;

    S3NObjectListingChunk(StorageObjectsChunk chunk) throws IOException {
      mChunk = chunk;
      if (mChunk == null) {
        throw new IOException("S3N listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      StorageObject[] objects = mChunk.getObjects();
      ObjectStatus[] ret = new ObjectStatus[objects.length];
      for (int i = 0; i < ret.length; ++i) {
        ret[i] = new ObjectStatus(objects[i].getKey(), objects[i].getContentLength(),
            objects[i].getLastModifiedDate().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      return mChunk.getCommonPrefixes();
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (!mChunk.isListingComplete()) {
        StorageObjectsChunk nextChunk =
            getObjectListingChunk(mChunk.getPrefix(), mChunk.getDelimiter(),
                mChunk.getPriorLastKey());
        if (nextChunk != null) {
          return new S3NObjectListingChunk(nextChunk);
        }
      }
      return null;
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      StorageObject meta = mClient.getObjectDetails(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getContentLength(), meta.getLastModifiedDate().getTime());
    } catch (ServiceException e) {
      return null;
    }
  }

  // No group in S3 ACL, returns the account owner for group.
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions(mAccountOwner, mAccountOwner, mBucketMode);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_S3N + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new S3InputStream(mBucketName, key, mClient, options.getOffset());
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
