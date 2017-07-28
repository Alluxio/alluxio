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
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.model.StorageObject;
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * GCS FS {@link UnderFileSystem} implementation based on the jets3t library.
 */
@ThreadSafe
public class GCSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GCSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  private static final byte[] DIR_HASH;

  /** Jets3t GCS client. */
  private final GoogleStorageService mClient;

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
   * Constructs a new instance of {@link GCSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link GCSUnderFileSystem} instance
   * @throws ServiceException when a connection to GCS could not be created
   */
  public static GCSUnderFileSystem createInstance(
      AlluxioURI uri, UnderFileSystemConfiguration conf) throws ServiceException {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.GCS_ACCESS_KEY),
            "Property " + PropertyKey.GCS_ACCESS_KEY + " is required to connect to GCS");
    Preconditions.checkArgument(conf.containsKey(PropertyKey.GCS_SECRET_KEY),
            "Property " + PropertyKey.GCS_SECRET_KEY + " is required to connect to GCS");
    GSCredentials googleCredentials = new GSCredentials(
        conf.getValue(PropertyKey.GCS_ACCESS_KEY),
        conf.getValue(PropertyKey.GCS_SECRET_KEY));

    // TODO(chaomin): maybe add proxy support for GCS.
    GoogleStorageService googleStorageService = new GoogleStorageService(googleCredentials);

    String accountOwnerId = googleStorageService.getAccountOwner().getId();
    // Gets the owner from user-defined static mapping from GCS account id to Alluxio user name.
    String owner = CommonUtils.getValueFromStaticMapping(
        conf.getValue(PropertyKey.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING), accountOwnerId);
    // If there is no user-defined mapping, use the display name.
    if (owner == null) {
      owner = googleStorageService.getAccountOwner().getDisplayName();
    }
    String accountOwner = owner == null ? accountOwnerId : owner;

    GSAccessControlList acl = googleStorageService.getBucketAcl(bucketName);
    short bucketMode = GCSUtils.translateBucketAcl(acl, accountOwnerId);

    return new GCSUnderFileSystem(uri, googleStorageService, bucketName, bucketMode, accountOwner,
        conf);
  }

  /**
   * Constructor for {@link GCSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param googleStorageService the Jets3t GCS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketMode the permission mode that the account owner has to the bucket
   * @param accountOwner the name of the account owner
   * @param conf configuration for this UFS
   */
  protected GCSUnderFileSystem(AlluxioURI uri, GoogleStorageService googleStorageService,
      String bucketName, short bucketMode, String accountOwner, UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = googleStorageService;
    mBucketName = bucketName;
    mBucketMode = bucketMode;
    mAccountOwner = accountOwner;
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
  protected boolean createEmptyObject(String key) {
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
    return new GCSOutputStream(mBucketName, key, mClient);
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
      return new GCSObjectListingChunk(chunk);
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
  protected ObjectStatus getObjectStatus(String key) {
    try {
      GSObject meta = mClient.getObjectDetails(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getContentLength(), meta.getLastModifiedDate().getTime());
    } catch (ServiceException e) {
      return null;
    }
  }

  // No group in GCS ACL, returns the account owner for group.
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions(mAccountOwner, mAccountOwner, mBucketMode);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_GCS + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new GCSInputStream(mBucketName, key, mClient, options.getOffset());
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
