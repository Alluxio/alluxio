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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
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
import java.io.FileNotFoundException;
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
public final class GCSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
   * @return the created {@link GCSUnderFileSystem} instance
   * @throws ServiceException when a connection to GCS could not be created
   */
  public static GCSUnderFileSystem createInstance(AlluxioURI uri)
      throws ServiceException {
    String bucketName = uri.getHost();
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.GCS_ACCESS_KEY),
        "Property " + PropertyKey.GCS_ACCESS_KEY + " is required to connect to GCS");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.GCS_SECRET_KEY),
        "Property " + PropertyKey.GCS_SECRET_KEY + " is required to connect to GCS");
    GSCredentials googleCredentials = new GSCredentials(
        Configuration.get(PropertyKey.GCS_ACCESS_KEY),
        Configuration.get(PropertyKey.GCS_SECRET_KEY));

    // TODO(chaomin): maybe add proxy support for GCS.
    GoogleStorageService googleStorageService = new GoogleStorageService(googleCredentials);

    String accountOwnerId = googleStorageService.getAccountOwner().getId();
    // Gets the owner from user-defined static mapping from GCS account id to Alluxio user name.
    String owner = CommonUtils.getValueFromStaticMapping(
        Configuration.get(PropertyKey.UNDERFS_GCS_OWNER_ID_TO_USERNAME_MAPPING), accountOwnerId);
    // If there is no user-defined mapping, use the display name.
    if (owner == null) {
      owner = googleStorageService.getAccountOwner().getDisplayName();
    }
    String accountOwner = owner == null ? accountOwnerId : owner;

    GSAccessControlList acl = googleStorageService.getBucketAcl(bucketName);
    short bucketMode = GCSUtils.translateBucketAcl(acl, accountOwnerId);

    return new GCSUnderFileSystem(uri, googleStorageService, bucketName, bucketMode, accountOwner);
  }

  /**
   * Constructor for {@link GCSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param googleStorageService the Jets3t GCS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketPrefix prefix of the bucket
   * @param bucketMode the permission mode that the account owner has to the bucket
   * @param accountOwner the name of the account owner
   */
  protected GCSUnderFileSystem(AlluxioURI uri,
      GoogleStorageService googleStorageService,
      String bucketName,
      short bucketMode,
      String accountOwner) {
    super(uri);
    mClient = googleStorageService;
    mBucketName = bucketName;
    mBucketMode = bucketMode;
    mAccountOwner = accountOwner;
  }

  @Override
  public String getUnderFSType() {
    return "gcs";
  }

  @Override
  protected OutputStream createOutputStream(String path) throws IOException {
    return new GCSOutputStream(mBucketName, stripPrefixIfPresent(path), mClient);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    if (!options.isRecursive()) {
      UnderFileStatus[] children = listInternal(path, false);
      if (children == null) {
        LOG.error("Unable to delete {} because listInternal returns null", path);
        return false;
      }
      if (children.length != 0) {
        LOG.error("Unable to delete {} because it is a non empty directory. Specify "
                + "recursive as true in order to delete non empty directories.", path);
        return false;
      }
    } else {
      // Delete children
      UnderFileStatus[] pathsToDelete = listInternal(path, true);
      if (pathsToDelete == null) {
        LOG.error("Unable to delete {} because listInternal returns null", path);
        return false;
      }
      for (UnderFileStatus pathToDelete : pathsToDelete) {
        // If we fail to deleteInternal one file, stop
        String pathKey = PathUtils.concatPath(path, pathToDelete.getName());
        boolean success;
        if (pathToDelete.isDirectory()) {
          success = deleteInternal(convertToFolderName(pathKey));
        } else {
          success = deleteInternal(pathKey);
        }
        if (!success) {
          LOG.error("Failed to delete path {}, aborting delete.", pathToDelete.getName());
          return false;
        }
      }
    }
    // Delete the directory itself
    return deleteInternal(convertToFolderName(path));
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return deleteInternal(path);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    GSObject details = getObjectDetails(path);
    if (details != null) {
      return details.getContentLength();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    GSObject details = getObjectDetails(path);
    if (details != null) {
      return details.getLastModifiedDate().getTime();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public boolean isDirectory(String key) {
    // Root is always a folder
    if (isRoot(key)) {
      return true;
    }
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      mClient.getObjectDetails(mBucketName, keyAsFolder);
      // If no exception is thrown, the key exists as a folder
      return true;
    } catch (ServiceException s) {
      // It is possible that the folder has not been encoded as a _$folder$ file
      try {
        String path = PathUtils.normalizePath(stripPrefixIfPresent(key), PATH_SEPARATOR);
        // Check if anything begins with <path>/
        GSObject[] objs = mClient.listObjects(mBucketName, path, "");
        if (objs.length > 0) {
          mkdirsInternal(path);
          return true;
        } else {
          return false;
        }
      } catch (ServiceException s2) {
        return false;
      }
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    try {
      return mClient.getObjectDetails(mBucketName, stripPrefixIfPresent(path)) != null;
    } catch (ServiceException e) {
      return false;
    }
  }

  @Override
  public String[] list(String path) throws IOException {
    // if the path not exists, or it is a file, then should return null
    if (!isDirectory(path)) {
      return null;
    }
    // Non recursive list
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    return UnderFileStatus.toListingResult(listInternal(path, false));
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, MkdirsOptions.defaults().setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    if (path == null) {
      return false;
    }
    if (isDirectory(path)) {
      return true;
    }
    if (isFile(path)) {
      LOG.error("Cannot create directory {} because it is already a file.", path);
      return false;
    }
    if (!options.getCreateParent()) {
      if (parentExists(path)) {
        // Parent directory exists
        return mkdirsInternal(path);
      } else {
        LOG.error("Cannot create directory {} because parent does not exist", path);
        return false;
      }
    }
    // Parent directories should be created
    if (parentExists(path)) {
      // Parent directory exists
      return mkdirsInternal(path);
    } else {
      String parentKey = getParentKey(path);
      // Recursively make the parent folders
      return mkdirs(parentKey, true) && mkdirsInternal(path);
    }
  }

  @Override
  public InputStream open(String path) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      return new GCSInputStream(mBucketName, path, mClient);
    } catch (ServiceException e) {
      LOG.error("Failed to open file: {}", path, e);
      return null;
    }
  }

  /**
   * Opens a GCS object at given position and returns the opened input stream.
   *
   * @param path the GCS object path
   * @param pos the position to open at
   * @return the opened input stream
   * @throws IOException if failed to open file at position
   */
  public InputStream openAtPosition(String path, long pos) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      return new GCSInputStream(mBucketName, path, mClient, pos);
    } catch (ServiceException e) {
      LOG.error("Failed to open file {} at position {}:", path, pos, e);
      return null;
    }
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    UnderFileStatus[] children = listInternal(src, false);
    if (children == null) {
      LOG.error("Failed to list directory {}, aborting rename.", src);
      return false;
    }
    if (isFile(dst) || isDirectory(dst)) {
      LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
      return false;
    }
    // Source exists and is a directory, and destination does not exist
    // Rename the source folder first
    if (!copy(convertToFolderName(src), convertToFolderName(dst))) {
      return false;
    }
    // Rename each child in the src folder to destination/child
    for (UnderFileStatus child : children) {
      String childSrcPath = PathUtils.concatPath(src, child.getName());
      String childDstPath = PathUtils.concatPath(dst, child.getName());
      boolean success;
      if (child.isDirectory()) {
        // Recursive call
        success = renameDirectory(childSrcPath, childDstPath);
      } else {
        success = renameFile(childSrcPath, childDstPath);
      }
      if (!success) {
        LOG.error("Failed to rename path {}, aborting rename.", child.getName());
        return false;
      }
    }
    // Delete src and everything under src
    return deleteDirectory(src, DeleteOptions.defaults().setRecursive(true));
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.error("Unable to rename {} to {} because source does not exist or is a directory.",
          src, dst);
      return false;
    }
    if (isFile(dst) || isDirectory(dst)) {
      LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
      return false;
    }
    // Source is a file and Destination does not exist
    return copy(src, dst) && deleteInternal(src);
  }

  // Setting GCS owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting GCS mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // Returns the account owner.
  @Override
  public String getOwner(String path) throws IOException {
    return mAccountOwner;
  }

  // No group in GCS ACL, returns the account owner.
  @Override
  public String getGroup(String path) throws IOException {
    return mAccountOwner;
  }

  // Returns the account owner's permission mode to the GCS bucket.
  @Override
  public short getMode(String path) throws IOException {
    return mBucketMode;
  }

  /**
   * Appends the directory suffix to the key.
   *
   * @param key the key to convert
   * @return key as a directory path
   */
  private String convertToFolderName(String key) {
    // Strips the slash if it is the end of the key string. This is because the slash at
    // the end of the string is not part of the Object key in GCS.
    if (key.endsWith(PATH_SEPARATOR)) {
      key = key.substring(0, key.length() - PATH_SEPARATOR.length());
    }
    return key + FOLDER_SUFFIX;
  }

  /**
   * Copies an object to another key.
   *
   * @param src the source key to copy
   * @param dst the destination key to copy to
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String src, String dst) {
    src = stripPrefixIfPresent(src);
    dst = stripPrefixIfPresent(dst);
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

  /**
   * Internal function to delete a key in GCS.
   *
   * @param key the key to delete
   * @return true if successful, false if an exception is thrown
   */
  private boolean deleteInternal(String key) {
    try {
      mClient.deleteObject(mBucketName, stripPrefixIfPresent(key));
    } catch (ServiceException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  /**
   * @param key the key to get the object details of
   * @return {@link GSObject} of the key, or null if the key does not exist
   */
  private GSObject getObjectDetails(String key) {
    try {
      if (isDirectory(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        return mClient.getObjectDetails(mBucketName, keyAsFolder);
      } else {
        return mClient.getObjectDetails(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException e) {
      return null;
    }
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
    StorageObjectsChunk mChunk;
    String mDelimiter;
    boolean mDone;
    String mPath;
    String mPriorLastKey;

    public GCSObjectListingResult(String path, boolean recursive) {
      mDelimiter = recursive ? "" : PATH_SEPARATOR;
      mDone = false;
      mPath = path;
      mPriorLastKey = null;
    }

    @Override
    public String[] getObjectNames() {
      StorageObject[] objects = mChunk.getObjects();
      String[] ret = new String[objects.length];
      for (int i = 0; i < objects.length; ++i) {
        ret[i] = objects[i].getKey();
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      return mChunk.getCommonPrefixes();
    }

    @Override
    public ObjectListingResult getNextChunk() throws IOException {
      if (mDone) {
        return null;
      }
      try {
        mChunk = mClient.listObjectsChunked(mBucketName, mPath, mDelimiter,
            LISTING_LENGTH, mPriorLastKey);
      } catch (ServiceException e) {
        LOG.error("Failed to list path {}", mPath, e);
        return null;
      }
      mDone = mChunk.isListingComplete();
      mPriorLastKey = mChunk.getPriorLastKey();
      return this;
    }
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_GCS + mBucketName;
  }

  @Override
  protected boolean mkdirsInternal(String key) {
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      GSObject obj = new GSObject(keyAsFolder);
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
}
