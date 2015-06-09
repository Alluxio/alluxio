/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.underfs.s3;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.Mimetypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;

/**
 * Under file system implementation for S3 using the Jets3t library.
 */
public class S3UnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory */
  private static final String FOLDER_SUFFIX = "_$folder$";
  /** Value used to indicate folder structure in S3 */
  private static final String PATH_SEPARATOR = "/";

  /** Jets3t S3 client */
  private final S3Service mClient;
  /** Bucket name of user's configured Tachyon bucket */
  private final String mBucketName;
  /** Prefix of the bucket, for example s3n://my-bucket-name/ */
  private final String mBucketPrefix;

  public S3UnderFileSystem(String bucketName, TachyonConf tachyonConf) throws ServiceException {
    super(tachyonConf);
    AWSCredentials awsCredentials =
        new AWSCredentials(tachyonConf.get(Constants.S3_ACCESS_KEY, null), tachyonConf.get(
            Constants.S3_SECRET_KEY, null));
    mBucketName = bucketName;
    mClient = new RestS3Service(awsCredentials);
    mBucketPrefix = Constants.HEADER_S3N + mBucketName + PATH_SEPARATOR;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) {
    // Authentication is taken care of in the constructor
  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) {
    // Authentication is taken care of in the constructor
  }

  @Override
  public OutputStream create(String path) throws IOException {
    if (mkdirs(getParentKey(path), true)) {
      return new S3OutputStream(mBucketName, stripPrefixIfPresent(path), mClient);
    }
    return null;
  }

  // Same as create(path)
  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    LOG.warn("Create with block size is not supported with S3UnderFileSystem. Block size will be "
        + "ignored.");
    return create(path);
  }

  // Same as create(path)
  @Override
  public OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    LOG.warn("Create with block size and replication is not supported with S3UnderFileSystem."
        + " Block size and replication will be ignored.");
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
      if (!deleteInternal(CommonUtils.concatPath(path, pathToDelete))) {
        LOG.error("Failed to delete path " + pathToDelete + ", aborting delete.");
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
   * There is no concept of a block in S3, however the maximum allowed size of one file is
   * currently 5 TB.
   * @param path The file name
   * @return 5 TB in bytes
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Constants.TB * 5;
  }

  // Not supported
  @Override
  public Object getConf() {
    LOG.warn("getConf is not supported when using S3UnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using S3UnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.warn("getFileLocations is not supported when using S3UnderFileSystem, returning null.");
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
    // Non recursive list
    path = path.endsWith(PATH_SEPARATOR) ? path : path + PATH_SEPARATOR;
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
      LOG.error("Cannot create directory " + path + " because it is already a file.");
      return false;
    }
    if (!createParent) {
      if (parentExists(path)) {
        // Parent directory exists
        return mkdirsInternal(path);
      } else {
        LOG.error("Cannot create directory " + path + " because parent does not exist");
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
    } catch (ServiceException se) {
      LOG.error("Failed to open file: " + path, se);
      return null;
    }
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    if (!exists(src)) {
      LOG.error("Unable to rename " + src + " to " + dst + " because source does not exist.");
      return false;
    }
    if (exists(dst)) {
      LOG.error("Unable to rename " + src + " to " + dst + " because destination already exists.");
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
        if (!rename(CommonUtils.concatPath(src, child), CommonUtils.concatPath(dst, child))) {
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

  // Not supported
  @Override
  public void setPermission(String path, String posixPerm) throws IOException {}

  /**
   * Appends the directory suffix to the key.
   * @param key the key to convert
   * @return key as a directory path
   */
  private String convertToFolderName(String key) {
    return key + FOLDER_SUFFIX;
  }

  /**
   * Copies an object to another key.
   * @param src the source key to copy.
   * @param dst the destination key to copy to.
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String src, String dst) {
    try {
      src = stripPrefixIfPresent(src);
      dst = stripPrefixIfPresent(dst);
      LOG.info("Copying " + src + " to " + dst);
      S3Object obj = new S3Object(dst);
      mClient.copyObject(mBucketName, src, mBucketName, obj, false);
      return true;
    } catch (ServiceException se) {
      LOG.error("Failed to rename file " + src + " to " + dst);
      return false;
    }
  }

  /**
   * Internal function to delete a key in S3
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
    } catch (ServiceException se) {
      LOG.error("Failed to delete " + key, se);
      return false;
    }
    return true;
  }

  /**
   * Gets the child name based on the parent name.
   * @param child the key of the child
   * @param parent the key of the parent
   * @return the child key with the parent prefix removed, null if the parent prefix is invalid
   */
  private String getChildName(String child, String parent) {
    if (child.startsWith(parent)) {
      return child.substring(parent.length());
    }
    LOG.error("Attempted to get childname with an invalid parent argument. Parent: " + parent
        + " Child: " + child);
    return null;
  }

  /**
   * Gets the StorageObject representing the metadata of a key. If the key does not exist as a
   * file or folder, null is returned
   * @param key the key to get the object details of
   * @return StorageObject of the key, or null if the key does not exist as a file or folder.
   */
  private StorageObject getObjectDetails(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
        return mClient.getObjectDetails(mBucketName, keyAsFolder);
      } else {
        return mClient.getObjectDetails(mBucketName, stripPrefixIfPresent(key));
      }
    } catch (ServiceException se) {
      return null;
    }
  }

  /**
   * Gets the parent key of the input key, or null if no parent exists
   * @param key the key to get the parent of
   * @return the the parent key
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
   * @param key the key to check
   * @return S3Object containing metadata
   */
  private boolean isFolder(String key) {
    key = key.endsWith(PATH_SEPARATOR) ? key.substring(0, key.length() - 1) : key;
    // Root is always a folder
    if (isRoot(key)) {
      return true;
    }
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      mClient.getObjectDetails(mBucketName, keyAsFolder);
      // If no exception is thrown, the key exists as a folder
      return true;
    } catch (ServiceException se) {
      return false;
    }
  }

  /**
   * Checks if the key is the root.
   * @param key the key to check
   * @return true if the key is the root, false otherwise
   */
  private boolean isRoot(String key) {
    return key.equals(Constants.HEADER_S3N + mBucketName)
        || key.equals(Constants.HEADER_S3N + mBucketName + PATH_SEPARATOR);
  }

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix
   * @param path the key to list
   * @param recursive if true will list children directories as well
   * @return an array of the file and folder names in this directory
   * @throws IOException
   */
  private String[] listInternal(String path, boolean recursive) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      path = path.endsWith(PATH_SEPARATOR) ? path : path + PATH_SEPARATOR;
      path = path.equals(PATH_SEPARATOR) ? "" : path;
      // Gets all the objects under the path, because we have no idea if there are non Tachyon
      // managed "directories"
      S3Object[] objs = mClient.listObjects(mBucketName, path, "");
      if (recursive) {
        String[] ret = new String[objs.length];
        for (int i = 0; i < objs.length; i ++) {
          // Remove parent portion of the key
          String child = getChildName(objs[i].getKey(), path);
          // Prune the special folder suffix
          child = stripFolderSuffixIfPresent(child);
          ret[i] = child;
        }
        return ret;
      }
      // Non recursive list
      Set<String> children = new HashSet<String>();
      for (S3Object obj : objs) {
        // Remove parent portion of the key
        String child = getChildName(obj.getKey(), path);
        // Remove any portion after the path delimiter
        int childNameIndex = child.indexOf(PATH_SEPARATOR);
        child = childNameIndex != -1 ? child.substring(0, childNameIndex) : child;
        // Prune the special folder suffix
        child = stripFolderSuffixIfPresent(child);
        // Add to the set of children, the set will deduplicate.
        children.add(child);
      }
      return children.toArray(new String[children.size()]);
    } catch (ServiceException se) {
      LOG.error("Failed to list path " + path);
      return null;
    }
  }

  /**
   * Creates a directory flagged file with the key and folder suffix.
   * @param key the key to create a folder
   * @return true if the operation was successful, false otherwise
   */
  private boolean mkdirsInternal(String key) {
    try {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
      S3Object obj = new S3Object(keyAsFolder);
      obj.setDataInputStream(new ByteArrayInputStream(new byte[0]));
      obj.setContentLength(0);
      obj.setContentType(Mimetypes.MIMETYPE_BINARY_OCTET_STREAM);
      mClient.putObject(mBucketName, obj);
      return true;
    } catch (ServiceException se) {
      LOG.error("Failed to create directory: " + key, se);
      return false;
    }
  }

  /**
   * Treating S3 as a file system, checks if the parent directory exists.
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
   * Strip the folder suffix if it exists. This is a string manipulation utility and does not
   * guarantee the existence of the folder. This method will leave keys without a suffix
   * unaltered.
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
   * Strips the s3 bucket prefix from the key if it is present. For example, for input key
   * s3n://my-bucket-name/my-path/file, the output would be my-path/file. This method will leave
   * keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   * @param key the key to strip
   * @return the key without the s3 bucket prefix
   */
  private String stripPrefixIfPresent(String key) {
    if (key.startsWith(mBucketPrefix)) {
      return key.substring(mBucketPrefix.length());
    }
    LOG.warn("Attempted to strip key with invalid prefix: " + key);
    return key;
  }
}
