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

package alluxio.underfs.gcs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.http.HttpStatus;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.model.StorageOwner;
import org.jets3t.service.security.GSCredentials;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Google Cloud Storage {@link UnderFileSystem} implementation based on the jets3t library.
 */
@ThreadSafe
public class GCSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GCSUnderFileSystem.class);

  /** Default owner of objects if owner cannot be determined. */
  private static final String DEFAULT_OWNER = "";

  private static final byte[] DIR_HASH;

  /** Jets3t GCS client. */
  private final GoogleStorageService mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The permissions associated with the bucket. Fetched once and assumed to be immutable. */
  private final Supplier<ObjectPermissions> mPermissions
      = CommonUtils.memoize(this::getPermissionsInternal);

  static {
    try {
      DIR_HASH = MessageDigest.getInstance("MD5").digest(new byte[0]);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Constructs a new instance of {@link GCSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link GCSUnderFileSystem} instance
   * @throws ServiceException when a connection to GCS could not be created
   */
  public static GCSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
      throws ServiceException {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.isSet(PropertyKey.GCS_ACCESS_KEY),
            "Property " + PropertyKey.GCS_ACCESS_KEY + " is required to connect to GCS");
    Preconditions.checkArgument(conf.isSet(PropertyKey.GCS_SECRET_KEY),
            "Property " + PropertyKey.GCS_SECRET_KEY + " is required to connect to GCS");
    GSCredentials googleCredentials = new GSCredentials(
        conf.get(PropertyKey.GCS_ACCESS_KEY),
        conf.get(PropertyKey.GCS_SECRET_KEY));

    // TODO(chaomin): maybe add proxy support for GCS.
    GoogleStorageService googleStorageService = new GoogleStorageService(googleCredentials);

    return new GCSUnderFileSystem(uri, googleStorageService, bucketName,
        conf);
  }

  /**
   * Constructor for {@link GCSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param googleStorageService the Jets3t GCS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected GCSUnderFileSystem(AlluxioURI uri, GoogleStorageService googleStorageService,
      String bucketName, UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = googleStorageService;
    mBucketName = bucketName;
  }

  @Override
  public String getUnderFSType() {
    return "gcs";
  }

  // Setting GCS owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting GCS mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    return getDirectoryStatus(path);
  }

  // GCS provides strong global consistency for read-after-write and read-after-metadata-update
  @Override
  public InputStream openExistingFile(String path) throws IOException {
    return open(path);
  }

  // GCS provides strong global consistency for read-after-write and read-after-metadata-update
  @Override
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    return open(path, options);
  }

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    GSObject obj = new GSObject(dst);
    // Retry copy for a few times, in case some Jets3t or GCS internal errors happened during copy.
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
  public boolean createEmptyObject(String key) {
    try {
      GSObject obj = new GSObject(key);
      obj.setDataInputStream(new ByteArrayInputStream(new byte[0]));
      obj.setContentLength(0);
      obj.setMd5Hash(DIR_HASH);
      obj.setContentType(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      mClient.putObject(mBucketName, obj);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to create directory: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new GCSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS, ","));
  }

  @Override
  protected boolean deleteObject(String key) throws IOException {
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
    return mUfsConf.get(PropertyKey.UNDERFS_GCS_DIRECTORY_SUFFIX);
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
      return new GCSObjectListingChunk(chunk);
    }
    return null;
  }

  // Get next chunk of listing result.
  private StorageObjectsChunk getObjectListingChunk(String key, String delimiter,
      String priorLastKey) {
    StorageObjectsChunk res;
    try {
      res = mClient.listObjectsChunked(mBucketName, key, delimiter,
          getListingChunkLength(mUfsConf), priorLastKey);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", key, e);
      res = null;
    }
    return res;
  }

  /**
   * Wrapper over GCS {@link StorageObjectsChunk}.
   */
  private final class GCSObjectListingChunk implements ObjectListingChunk {
    final StorageObjectsChunk mChunk;

    GCSObjectListingChunk(StorageObjectsChunk chunk)
        throws IOException {
      mChunk = chunk;
      if (mChunk == null) {
        throw new IOException("GCS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      StorageObject[] objects = mChunk.getObjects();
      ObjectStatus[] ret = new ObjectStatus[objects.length];
      for (int i = 0; i < ret.length; ++i) {
        ret[i] = new ObjectStatus(objects[i].getKey(), objects[i].getMd5HashAsBase64(),
            objects[i].getContentLength(), objects[i].getLastModifiedDate().getTime());
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
        StorageObjectsChunk nextChunk = getObjectListingChunk(mChunk.getPrefix(),
            mChunk.getDelimiter(), mChunk.getPriorLastKey());
        if (nextChunk != null) {
          return new GCSObjectListingChunk(nextChunk);
        }
      }
      return null;
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) throws IOException {
    try {
      GSObject meta = mClient.getObjectDetails(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getMd5HashAsBase64(), meta.getContentLength(),
          meta.getLastModifiedDate().getTime());
    } catch (ServiceException e) {
      if (e.getResponseCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      }
      throw new IOException(e);
    }
  }

  @Override
  protected ObjectPermissions getPermissions() {
    return mPermissions.get();
  }

  /**
   * Since there is no group in GCS acl, the owner is reused as the group. This method calls the
   * GCS API and requires additional permissions aside from just read only. This method is best
   * effort and will continue with default permissions (no owner, no group, 0700).
   *
   * @return the permissions associated with this under storage system
   */
  private ObjectPermissions getPermissionsInternal() {
    // getAccountOwner() can return null even when the account is authenticated.
    // TODO(chaomin): investigate the root cause why Google cloud service is returning
    // null StorageOwner.
    String accountOwnerId = DEFAULT_OWNER;
    String accountOwner = DEFAULT_OWNER;
    try {
      StorageOwner storageOwner = mClient.getAccountOwner();
      if (storageOwner != null) {
        accountOwnerId = storageOwner.getId();
        // Gets the owner from user-defined static mapping from GCS account id to Alluxio user name.
        String owner = CommonUtils.getValueFromStaticMapping(
            mUfsConf.get(PropertyKey.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING), accountOwnerId);
        // If there is no user-defined mapping, use the display name.
        if (owner == null) {
          owner = storageOwner.getDisplayName();
        }
        accountOwner = owner == null ? accountOwnerId : owner;
      } else {
        LOG.debug("GoogleStorageService returns a null StorageOwner "
            + "with this Google Cloud account.");
      }
    } catch (ServiceException e) {
      LOG.warn("Failed to get Google account owner, proceeding with defaults owner {}. {}",
          accountOwner, e.toString());
    }

    short bucketMode =
        ModeUtils.getUMask(mUfsConf.get(PropertyKey.UNDERFS_GCS_DEFAULT_MODE)).toShort();
    try {
      GSAccessControlList acl = mClient.getBucketAcl(mBucketName);
      bucketMode = GCSUtils.translateBucketAcl(acl, accountOwnerId);
    } catch (ServiceException e) {
      LOG.warn("Failed to inherit bucket ACLs, proceeding with defaults. {}", e.toString());
    }

    // No group in GCS ACL, returns the account owner for group.
    return new ObjectPermissions(accountOwner, accountOwner, bucketMode);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_GCS + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy) {
    return new GCSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy);
  }
}
