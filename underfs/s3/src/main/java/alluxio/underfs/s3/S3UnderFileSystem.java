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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * S3 FS {@link UnderFileSystem} implementation based on the jets3t library.
 */
@ThreadSafe
public final class S3UnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Value used to indicate folder structure in S3. */
  private static final String PATH_SEPARATOR = "/";

  private static final byte[] DIR_HASH;

  /** Jets3t S3 client. */
  private final S3Service mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Prefix of the bucket, for example s3n://my-bucket-name/ . */
  private final String mBucketPrefix;

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
   * @return the created {@link S3UnderFileSystem} instance
   * @throws ServiceException when a connection to S3 could not be created
   */
  public static S3UnderFileSystem createInstance(AlluxioURI uri) throws ServiceException {
    String bucketName = uri.getHost();
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.S3N_ACCESS_KEY),
        "Property " + PropertyKey.S3N_ACCESS_KEY + " is required to connect to S3");
    Preconditions.checkArgument(Configuration.containsKey(PropertyKey.S3N_SECRET_KEY),
        "Property " + PropertyKey.S3N_SECRET_KEY + " is required to connect to S3");
    AWSCredentials awsCredentials = new AWSCredentials(
        Configuration.get(PropertyKey.S3N_ACCESS_KEY),
        Configuration.get(PropertyKey.S3N_SECRET_KEY));

    Jets3tProperties props = new Jets3tProperties();
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      props.setProperty("httpclient.proxy-autodetect", "false");
      props.setProperty(
          "httpclient.proxy-host", Configuration.get(PropertyKey.UNDERFS_S3_PROXY_HOST));
      props.setProperty(
          "httpclient.proxy-port", Configuration.get(PropertyKey.UNDERFS_S3_PROXY_PORT));
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY)) {
      props.setProperty("s3service.https-only",
          Boolean.toString(Configuration.getBoolean(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY)));
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_ENDPOINT)) {
      props.setProperty(
          "s3service.s3-endpoint", Configuration.get(PropertyKey.UNDERFS_S3_ENDPOINT));
      if (Configuration.getBoolean(PropertyKey.UNDERFS_S3_PROXY_HTTPS_ONLY)) {
        props.setProperty("s3service.s3-endpoint-https-port",
            Configuration.get(PropertyKey.UNDERFS_S3_ENDPOINT_HTTPS_PORT));
      } else {
        props.setProperty("s3service.s3-endpoint-http-port",
            Configuration.get(PropertyKey.UNDERFS_S3_ENDPOINT_HTTP_PORT));
      }
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS)) {
      props.setProperty("s3service.disable-dns-buckets",
          Configuration.get(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS));
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX)) {
      props.setProperty("threaded-service.max-thread-count",
          Configuration.get(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX));
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX)) {
      props.setProperty("threaded-service.admin-max-thread-count",
          Configuration.get(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX));
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_S3_THREADS_MAX)) {
      props.setProperty("httpclient.max-connections",
          Configuration.get(PropertyKey.UNDERFS_S3_THREADS_MAX));
    }
    LOG.debug("Initializing S3 underFs with properties: {}", props.getProperties());
    RestS3Service restS3Service = new RestS3Service(awsCredentials, null, null, props);
    String bucketPrefix = PathUtils.normalizePath(Constants.HEADER_S3N + bucketName,
        PATH_SEPARATOR);

    String accountOwnerId = restS3Service.getAccountOwner().getId();
    // Gets the owner from user-defined static mapping from S3 canonical user id to Alluxio
    // user name.
    String owner = CommonUtils.getValueFromStaticMapping(
        Configuration.get(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING),
        accountOwnerId);
    // If there is no user-defined mapping, use the display name.
    if (owner == null) {
      owner = restS3Service.getAccountOwner().getDisplayName();
    }
    String accountOwner = owner == null ? accountOwnerId : owner;

    AccessControlList acl = restS3Service.getBucketAcl(bucketName);
    short bucketMode = S3Utils.translateBucketAcl(acl, accountOwnerId);

    return new S3UnderFileSystem(uri, restS3Service, bucketName, bucketPrefix,
        bucketMode, accountOwner);
  }

  /**
   * Constructor for {@link S3UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param s3Service Jets3t S3 client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param bucketPrefix prefix of the bucket
   * @param bucketMode the permission mode that the account owner has to the bucket
   * @param accountOwner the name of the account owner
   */
  protected S3UnderFileSystem(AlluxioURI uri,
      S3Service s3Service,
      String bucketName,
      String bucketPrefix,
      short bucketMode,
      String accountOwner) {
    super(uri);
    mClient = s3Service;
    mBucketName = bucketName;
    mBucketPrefix = bucketPrefix;
    mBucketMode = bucketMode;
    mAccountOwner = accountOwner;
  }

  @Override
  public String getUnderFSType() {
    return "s3";
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(String hostname) {
    // Authentication is taken care of in the constructor
  }

  @Override
  public void connectFromWorker(String hostname) {
    // Authentication is taken care of in the constructor
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, new CreateOptions());
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (mkdirs(getParentKey(path), true)) {
      return new S3OutputStream(mBucketName, stripPrefixIfPresent(path), mClient);
    }
    return null;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    if (!recursive) {
      String[] children = listInternal(path, false);
      if (children == null) {
        LOG.error("Unable to delete {} because listInternal returns null", path);
        return false;
      }
      if (isFolder(path) && children.length != 0) {
        LOG.error("Unable to delete {} because it is a non empty directory. Specify "
                + "recursive as true in order to delete non empty directories.", path);
        return false;
      }
      return deleteInternal(path);
    }
    // Get all relevant files
    String[] pathsToDelete = listInternal(path, true);
    if (pathsToDelete == null) {
      LOG.error("Unable to delete {} because listInternal returns null", path);
      return false;
    }
    for (String pathToDelete : pathsToDelete) {
      // If we fail to deleteInternal one file, stop
      if (!deleteInternal(PathUtils.concatPath(path, pathToDelete))) {
        LOG.error("Failed to delete path {}, aborting delete.", pathToDelete);
        return false;
      }
    }
    return deleteInternal(path);
  }

  @Override
  public boolean exists(String path) throws IOException {
    // Root path always exists.
    return isRoot(path) || getObjectDetails(path) != null;
  }

  /**
   * Gets the block size in bytes. There is no concept of a block in S3 and the maximum size of
   * one put is 5 GB, and the max size of a multi-part upload is 5 TB. This method defaults to the
   * default user block size in Alluxio.
   *
   * @param path the file name
   * @return the default Alluxio user block size
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  // Not supported
  @Override
  public Object getConf() {
    LOG.debug("getConf is not supported when using S3UnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using S3UnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.debug("getFileLocations is not supported when using S3UnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    StorageObject details = getObjectDetails(path);
    if (details != null) {
      return details.getContentLength();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    StorageObject details = getObjectDetails(path);
    if (details != null) {
      return details.getLastModifiedDate().getTime();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return exists(path) && !isFolder(path);
  }

  @Override
  public String[] list(String path) throws IOException {
    // if the path not exists, or it is a file, then should return null
    if (!exists(path) || isFile(path)) {
      return null;
    }
    // Non recursive list
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    return listInternal(path, false);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions().setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    if (path == null) {
      return false;
    }
    if (isFolder(path)) {
      return true;
    }
    if (exists(path)) {
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
      return new S3InputStream(mBucketName, path, mClient);
    } catch (ServiceException e) {
      LOG.error("Failed to open file: {}", path, e);
      return null;
    }
  }

  /**
   * Opens a S3 object at given position and returns the opened input stream.
   *
   * @param path the S3 object path
   * @param pos the position to open at
   * @return the opened input stream
   * @throws IOException if failed to open file at position
   */
  public InputStream openAtPosition(String path, long pos) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      return new S3InputStream(mBucketName, path, mClient, pos);
    } catch (ServiceException e) {
      LOG.error("Failed to open file {} at position {}:", path, pos, e);
      return null;
    }
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    if (!exists(src)) {
      LOG.error("Unable to rename {} to {} because source does not exist.", src, dst);
      return false;
    }
    if (exists(dst)) {
      LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
      return false;
    }
    // Source exists and destination does not exist
    if (isFolder(src)) {
      // Rename the source folder first
      if (!copy(convertToFolderName(src), convertToFolderName(dst))) {
        return false;
      }
      // Rename each child in the src folder to destination/child
      String [] children = list(src);
      if (children == null) {
        LOG.error("Failed to list path {}, aborting rename.", src);
        return false;
      }
      for (String child : children) {
        if (!rename(PathUtils.concatPath(src, child), PathUtils.concatPath(dst, child))) {
          LOG.error("Failed to rename path {}, aborting rename.", child);
          return false;
        }
      }
      // Delete src and everything under src
      return delete(src, true);
    }
    // Source is a file and Destination does not exist
    return copy(src, dst) && deleteInternal(src);
  }

  // Not supported
  @Override
  public void setConf(Object conf) {}

  // Setting S3 owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting S3 mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // Returns the account owner.
  @Override
  public String getOwner(String path) throws IOException {
    return mAccountOwner;
  }

  // No group in S3 ACL, returns the account owner.
  @Override
  public String getGroup(String path) throws IOException {
    return mAccountOwner;
  }

  // Returns the account owner's permission mode to the S3 bucket.
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
    // the end of the string is not part of the Object key in S3.
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

  /**
   * Internal function to delete a key in S3.
   *
   * @param key the key to delete
   * @return true if successful, false if an exception is thrown
   */
  private boolean deleteInternal(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        mClient.deleteObject(mBucketName, keyAsFolder);
      } else {
        mClient.deleteObject(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  /**
   * Gets the child name based on the parent name.
   *
   * @param child the key of the child
   * @param parent the key of the parent
   * @return the child key with the parent prefix removed, null if the parent prefix is invalid
   */
  private String getChildName(String child, String parent) {
    if (child.startsWith(parent)) {
      return child.substring(parent.length());
    }
    LOG.error("Attempted to get childname with an invalid parent argument. Parent: {} Child: {}",
        parent, child);
    return null;
  }

  /**
   * @param key the key to get the object details of
   * @return {@link StorageObject} of the key, or null if the key does not exist
   */
  private StorageObject getObjectDetails(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        return mClient.getObjectDetails(mBucketName, keyAsFolder);
      } else {
        return mClient.getObjectDetails(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException e) {
      return null;
    }
  }

  /**
   * @param key the key to get the parent of
   * @return the parent key, or null if the parent does not exist
   */
  private String getParentKey(String key) {
    // Root does not have a parent.
    if (isRoot(key)) {
      return null;
    }
    int separatorIndex = key.lastIndexOf(PATH_SEPARATOR);
    if (separatorIndex < 0) {
      return null;
    }
    return key.substring(0, separatorIndex);
  }

  /**
   * Determines if the key represents a folder. If false is returned, it is not guaranteed that the
   * path exists.
   *
   * @param key the key to check
   * @return whether the given key identifies a folder
   */
  private boolean isFolder(String key) {
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
        S3Object[] objs = mClient.listObjects(mBucketName, path, "");
        // If there are, this is a folder and we can create the necessary metadata
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

  /**
   * Checks if the key is the root.
   *
   * @param key the key to check
   * @return true if the key is the root, false otherwise
   */
  private boolean isRoot(String key) {
    return PathUtils.normalizePath(key, PATH_SEPARATOR).equals(
        PathUtils.normalizePath(Constants.HEADER_S3N + mBucketName, PATH_SEPARATOR));
  }

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix. Note that, the list results are unsorted.
   *
   * @param path the key to list
   * @param recursive if true will list children directories as well
   * @return an array of the file and folder names in this directory
   * @throws IOException if an I/O error occurs
   */
  private String[] listInternal(String path, boolean recursive) throws IOException {
    path = stripPrefixIfPresent(path);
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    path = path.equals(PATH_SEPARATOR) ? "" : path;
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    String priorLastKey = null;
    Set<String> children = new HashSet<>();
    try {
      boolean done = false;
      while (!done) {
        // Directories in S3 UFS can be possibly encoded in two different ways:
        // (1) as file objects with FOLDER_SUFFIX for directories created through Alluxio or
        // (2) as "common prefixes" of other files objects for directories not created through
        // Alluxio
        //
        // Case (1) (and file objects) is accounted for by iterating over chunk.getObjects() while
        // case (2) is accounted for by iterating over chunk.getCommonPrefixes().
        //
        // An example, with prefix="ufs" and delimiter="/" and LISTING_LENGTH=5
        // - objects.key = ufs/, child =
        // - objects.key = ufs/dir1_$folder$, child = dir1
        // - objects.key = ufs/file, child = file
        // - commonPrefix = ufs/dir1/, child = dir1
        // - commonPrefix = ufs/dir2/, child = dir2
        StorageObjectsChunk chunk = mClient.listObjectsChunked(mBucketName, path, delimiter,
            LISTING_LENGTH, priorLastKey);

        // Handle case (1)
        for (StorageObject obj : chunk.getObjects()) {
          // Remove parent portion of the key
          String child = getChildName(obj.getKey(), path);
          // Prune the special folder suffix
          child = CommonUtils.stripSuffixIfPresent(child, FOLDER_SUFFIX);
          // Only add if the path is not empty (removes results equal to the path)
          if (!child.isEmpty()) {
            children.add(child);
          }
        }
        // Handle case (2)
        for (String commonPrefix : chunk.getCommonPrefixes()) {
          // Remove parent portion of the key
          String child = getChildName(commonPrefix, path);

          if (child != null) {
            // Remove any portion after the last path delimiter
            int childNameIndex = child.lastIndexOf(PATH_SEPARATOR);
            child = childNameIndex != -1 ? child.substring(0, childNameIndex) : child;
            if (!child.isEmpty() && !children.contains(child)) {
              // This directory has not been created through Alluxio.
              mkdirsInternal(commonPrefix);
              children.add(child);
            }
          }
        }
        done = chunk.isListingComplete();
        priorLastKey = chunk.getPriorLastKey();
      }
      return children.toArray(new String[children.size()]);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", path, e);
      return null;
    }
  }

  /**
   * Creates a directory flagged file with the key and folder suffix.
   *
   * @param key the key to create a folder
   * @return true if the operation was successful, false otherwise
   */
  private boolean mkdirsInternal(String key) {
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      S3Object obj = new S3Object(keyAsFolder);
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

  /**
   * Treating S3 as a file system, checks if the parent directory exists.
   *
   * @param key the key to check
   * @return true if the parent exists or if the key is root, false otherwise
   */
  private boolean parentExists(String key) {
    // Assume root always has a parent
    if (isRoot(key)) {
      return true;
    }
    String parentKey = getParentKey(key);
    return parentKey != null && isFolder(parentKey);
  }

  /**
   * Strips the s3 bucket prefix or the preceding path separator from the key if it is present. For
   * example, for input key s3n://my-bucket-name/my-path/file, the output would be my-path/file. If
   * key is an absolute path like /my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param key the key to strip
   * @return the key without the s3 bucket prefix
   */
  private String stripPrefixIfPresent(String key) {
    String stripedKey = CommonUtils.stripPrefixIfPresent(key, mBucketPrefix);
    if (!stripedKey.equals(key)) {
      return stripedKey;
    }
    return CommonUtils.stripPrefixIfPresent(key, PATH_SEPARATOR);
  }

  @Override
  public boolean supportsFlush() {
    return false;
  }
}
