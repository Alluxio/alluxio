/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Aliyun OSS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public final class OSSUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Value used to indicate folder structure in OSS. */
  private static final String PATH_SEPARATOR = "/";

  /** Aliyun OSS client. */
  private final OSSClient mOssClient;

  /** The accessId to connect OSS. */
  private final String mAccessId;

  /** The accessKey to connect OSS. */
  private final String mAccessKey;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Prefix of the bucket, for example oss://bucket-name/ . */
  private final String mBucketPrefix;

  /** The OSS endpoint. */
  private final String mEndPoint;

  protected OSSUnderFileSystem(AlluxioURI uri, Configuration configuration) throws Exception {
    super(uri, configuration);
    String bucketName = uri.getHost();
    Preconditions.checkArgument(configuration.containsKey(Constants.OSS_ACCESS_KEY),
        "Property " + Constants.OSS_ACCESS_KEY + " is required to connect to OSS");
    Preconditions.checkArgument(configuration.containsKey(Constants.OSS_SECRET_KEY),
        "Property " + Constants.OSS_SECRET_KEY + " is required to connect to OSS");
    Preconditions.checkArgument(configuration.containsKey(Constants.OSS_ENDPOINT_KEY),
        "Property " + Constants.OSS_ENDPOINT_KEY + " is required to connect to OSS");
    mAccessId = configuration.get(Constants.OSS_ACCESS_KEY);
    mAccessKey = configuration.get(Constants.OSS_SECRET_KEY);
    mBucketName = bucketName;
    mBucketPrefix = Constants.HEADER_OSS + mBucketName + PATH_SEPARATOR;
    mEndPoint = configuration.get(Constants.OSS_ENDPOINT_KEY);

    ClientConfiguration ossClientConf = initializeOSSClientConfig(configuration);
    mOssClient = new OSSClient(mEndPoint, mAccessId, mAccessKey, ossClientConf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.OSS;
  }

  @Override
  public void connectFromMaster(Configuration conf, String hostname) throws IOException {
    // Authentication is taken care of in the constructor
  }

  @Override
  public void connectFromWorker(Configuration conf, String hostname) throws IOException {
    // Authentication is taken care of in the constructor
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path) throws IOException {
    path = toURIPath(path);
    if (mkdirs(getParentKey(path), true)) {
      return new OSSOutputStream(mBucketName, stripPrefixIfPresent(path), mOssClient);
    }
    return null;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    LOG.warn("blocksize is not supported with OSSUnderFileSystem. Block size will be ignored.");
    return create(path);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    LOG.warn(
        "blocksize and replication is not supported with OSSUnderFileSystem. Will be ignored.");
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    if (!recursive) {
      if (isFolder(path) && listInternal(path, false).length != 0) {
        LOG.error("Unable to delete " + path + " because it is a non empty directory. Specify "
            + "recursive as true in order to delete non empty directories.");
        return false;
      }
      return deleteInternal(path);
    }
    // Get all relevant files
    String[] pathsToDelete = listInternal(path, true);
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
    return isRoot(path) || getObjectDetails(path) != null;
  }

  /**
   * Gets the block size in bytes. There is no concept of a block in OSS and the maximum size of
   * one put is 5 GB and the maximum size of a multipart upload is 48.8 TB. This method defaults to
   * the default user block size in Alluxio.
   *
   * @param path the file name
   * @return the default Alluxio user block size
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mConfiguration.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  // Not supported
  @Override
  public Object getConf() {
    LOG.warn("getConf is not supported when using OSSUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using OSSUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.warn("getFileLocations is not supported when using OSSUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    ObjectMetadata objectMeta = getObjectDetails(path);
    if (objectMeta != null) {
      return objectMeta.getContentLength();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    ObjectMetadata objectMeta = getObjectDetails(path);
    if (objectMeta != null) {
      return objectMeta.getLastModified().getTime();
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
    if (!createParent) {
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
      return new OSSInputStream(mBucketName, path, mOssClient);
    } catch (ServiceException e) {
      LOG.error("Failed to open file: {}", path, e);
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
      for (String child: children) {
        if (!rename(PathUtils.concatPath(src, child), PathUtils.concatPath(dst, child))) {
          return false;
        }
      }
      // Delete src and everything under src
      return delete(src, true);
    }
    // Source is a file and Destination does not exist
    return copy(src, dst) && deleteInternal(src);
  }

  @Override
  public void setConf(Object conf) {
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
  }

  /**
   * Appends the directory suffix to the key.
   *
   * @param key the key to convert
   * @return key as a directory path
   */
  private String convertToFolderName(String key) {
    // Strips the slash if it is the end of the key string. This is because the slash at
    // the end of the string is not part of the Object key in OSS.
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
    try {
      src = stripPrefixIfPresent(src);
      dst = stripPrefixIfPresent(dst);
      LOG.info("Copying {} to {}", src, dst);
      mOssClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  /**
   * Internal function to delete a key in OSS.
   *
   * @param key the key to delete
   * @return true if successful, false if an exception is thrown
   */
  private boolean deleteInternal(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        mOssClient.deleteObject(mBucketName, keyAsFolder);
      } else {
        mOssClient.deleteObject(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  /**
   * @param key the key to get the object details of
   * @return {@link ObjectMetadata} of the key, or null if the key does not exist
   */
  private ObjectMetadata getObjectDetails(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        return mOssClient.getObjectMetadata(mBucketName, keyAsFolder);
      } else {
        return mOssClient.getObjectMetadata(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException e) {
      LOG.warn("Failed to get Object {}, return null", key, e);
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
   * Creates an OSS {@code ClientConfiguration} using an Alluxio configuration.
   *
   * @param configuration Alluxio configuration
   * @return the OSS {@link ClientConfiguration}
   */
  private ClientConfiguration initializeOSSClientConfig(Configuration configuration) {
    ClientConfiguration ossClientConf = new ClientConfiguration();
    ossClientConf.setConnectionTimeout(
        configuration.getInt(Constants.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout(
        configuration.getInt(Constants.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setConnectionTTL(configuration.getLong(Constants.UNDERFS_OSS_CONNECT_TTL));
    ossClientConf.setMaxConnections(configuration.getInt(Constants.UNDERFS_OSS_CONNECT_MAX));
    return ossClientConf;
  }

  /**
   * Determines if the key represents a folder. If false is returned, it is not guaranteed that the
   * path exists.
   *
   * @param key the key to check
   * @return true if the key exists and is a directory
   */
  private boolean isFolder(String key) {
    // Root is a folder
    if (isRoot(key)) {
      return true;
    }
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      mOssClient.getObjectMetadata(mBucketName, keyAsFolder);
      return true;
    } catch (ServiceException e) {
      // exception is thrown, the key is not exists, also not a folder
      return false;
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
          PathUtils.normalizePath(Constants.HEADER_OSS + mBucketName, PATH_SEPARATOR));
  }

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix.
   *
   * @param path the key to list
   * @param recursive if true will list children directories as well
   * @return an array of the file and folder names in this directory
   * @throws IOException if an I/O error occurs
   */
  private String[] listInternal(String path, boolean recursive) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      path = PathUtils.normalizePath(path, PATH_SEPARATOR);
      path = path.equals(PATH_SEPARATOR) ? "" : path;
      // Gets all the objects under the path, because we have no idea if there are non Alluxio
      // managed "directories"
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest(mBucketName);
      listObjectsRequest.setPrefix(path);

      // recursive, so don't set the delimiter
      // then list will return all files in this dir and subdirs
      if (recursive) {
        ObjectListing listing = mOssClient.listObjects(listObjectsRequest);
        List<OSSObjectSummary> objectSummaryList = listing.getObjectSummaries();
        String[] ret = new String[objectSummaryList.size()];
        for (int i = 0; i < objectSummaryList.size(); i++) {
          // Remove parent portion of the key
          String child = getChildName(objectSummaryList.get(i).getKey(), path);
          // Prune the special folder suffix
          child = stripFolderSuffixIfPresent(child);
          ret[i] = child;
        }
        return ret;
      }

      // Non recursive, so set the delimiter, let the listObjects only get the files in the folder
      listObjectsRequest.setDelimiter(PATH_SEPARATOR);
      Set<String> children = new HashSet<String>();
      ObjectListing listing = mOssClient.listObjects(listObjectsRequest);
      for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
        // Remove parent portion of the key
        String child = getChildName(objectSummary.getKey(), path);
        // Remove any portion after the path delimiter
        int childNameIndex = child.indexOf(PATH_SEPARATOR);
        child = childNameIndex != -1 ? child.substring(0, childNameIndex) : child;
        // Prune the special folder suffix
        child = stripFolderSuffixIfPresent(child);
        // Add to the set of children, the set will deduplicate.
        children.add(child);
      }
      return children.toArray(new String[children.size()]);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", path, e);
      return null;
    }
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
   * Creates a directory flagged file with the key and folder suffix.
   *
   * @param key the key to create a folder
   * @return true if the operation was successful, false otherwise
   */
  private boolean mkdirsInternal(String key) {
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      mOssClient.putObject(
          mBucketName, keyAsFolder,
          new ByteArrayInputStream(new byte[0]),
          objMeta);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to create directory: {}", key, e);
      return false;
    }

  }

  /**
   * Treating OSS as a file system, checks if the parent directory exists.
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
   * If the path passed to this filesystem is not an URI path, then add oss prefix.
   *
   * @param path the path to process
   * @return the path with oss prefix
   */
  private String toURIPath(String path) {
    if (!path.startsWith(Constants.HEADER_OSS)) {
      path = Constants.HEADER_OSS + mBucketName + path;
    }
    return path;
  }

  /**
   * Strips the folder suffix if it exists. This is a string manipulation utility and does not
   * guarantee the existence of the folder. This method will leave keys without a suffix unaltered.
   *
   * @param key the key to strip the suffix from
   * @return the key with the suffix removed, or the key unaltered if the suffix is not present
   */
  private String stripFolderSuffixIfPresent(String key) {
    if (key.endsWith(FOLDER_SUFFIX)) {
      return key.substring(0, key.length() - FOLDER_SUFFIX.length());
    }
    return key;
  }

  /**
   * Strips the OSS bucket prefix or the preceding path separator from the key if it is present. For
   * example, for input key oss://my-bucket-name/my-path/file, the output would be my-path/file. If
   * key is an absolute path like /my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param key the key to strip
   * @return the key without the oss bucket prefix
   */
  private String stripPrefixIfPresent(String key) {
    if (key.startsWith(mBucketPrefix)) {
      return key.substring(mBucketPrefix.length());
    }
    if (key.startsWith(PATH_SEPARATOR)) {
      return key.substring(PATH_SEPARATOR.length());
    }
    return key;
  }
}
