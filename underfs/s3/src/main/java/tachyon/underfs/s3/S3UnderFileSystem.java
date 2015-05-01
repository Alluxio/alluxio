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
import java.util.List;

import com.google.common.base.Throwables;

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
  private static final String FOLDER_SUFFIX = "_$folder$";
  private static final String PATH_SEPARATOR = "/";
  private static final String SCHEME = "s3n://";

  private final S3Service mClient;
  private final String mBucketName;

  public S3UnderFileSystem(String bucketName, TachyonConf tachyonConf) {
    super(tachyonConf);
    AWSCredentials awsCredentials =
        new AWSCredentials(tachyonConf.get("fs.s3n.awsAccessKeyId", null), tachyonConf.get(
            "fs.s3n.awsSecretAccessKey", null));
    mBucketName = bucketName;
    mClient = new RestS3Service(awsCredentials);
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
      return new S3OutputStream(mBucketName, path, mClient);
    }
    return null;
  }

  // Same as create(path)
  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path);
  }

  // Same as create(path)
  @Override
  public OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    if (!recursive) {
      return deleteInternal(path);
    }

    // Get all relevant files
    String[] pathsToDelete = listInternal(path, true);
    for (String pathToDelete : pathsToDelete) {
      // If we fail to deleteInternal one file, stop
      if (!deleteInternal(path + pathToDelete)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // Root path always exists.
    return isRoot(path) || getObjectDetails(path) != null;
  }

  // Largest size of one file is 5 TB
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Constants.TB * 5;
  }

  // Not supported
  @Override
  public Object getConf() {
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
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
    if (exists(path)) {
      return !isFolder(path);
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public String[] list(String path) throws IOException {
    // Non recursive list
    return listInternal(path, false);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
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
        return mkdir(path);
      } else {
        LOG.error("Cannot create directory " + path + " because parent does not exist");
        return false;
      }
    }
    // Parent directories should be created
    if (parentExists(path)) {
      // Parent directory exists
      return mkdir(path);
    } else {
      String parentKey = getParentKey(path);
      // Recursively make the parent folders
      return mkdirs(parentKey, true) && mkdir(parentKey);
    }
  }

  @Override
  public InputStream open(String path) throws IOException {
    try {
      path = path.substring(SCHEME.length() + mBucketName.length() + 1);
      return mClient.getObject(mBucketName, path).getDataInputStream();
    } catch (ServiceException se) {
      LOG.error("Failed to open file: " + path, se);
      return null;
    }
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    if (!exists(src)) {
      return false;
    }
    if (exists(dst)) {
      if (!isFolder(dst)) {
        LOG.error("Unable to rename " + src + " to " + dst + " because destination already "
            + "exists as a file.");
        return false;
      }
      String srcName = getKeyName(src);
      // Destination is a folder, move source into dst
      if (!isFolder(src)) {
        // Source is a file
        // Copy to destination
        if (copy(src, CommonUtils.concatPath(dst, srcName))) {
          // Delete original
          return deleteInternal(src);
        } else {
          return false;
        }
      }
      // Source and Destination are folders
      // Rename the source folder first
      String dstFolder = CommonUtils.concatPath(dst, convertToFolderName(srcName));
      if (!copy(convertToFolderName(src), dstFolder)) {
        return false;
      }
      // Rename each child in the src folder to destination/srcfolder/child
      String [] children = list(src);
      String parentName = getKeyName(getParentKey(src));
      for (String childKey : children) {
        if (!rename(childKey, CommonUtils.concatPath(dst, parentName))) {
          return false;
        }
      }
      // Delete everything under src
      return delete(src, true);
    }
    // Destination does not exist
    if (isFolder(src)) {
      // Rename the source folder first
      if (!copy(convertToFolderName(src), convertToFolderName(dst))) {
        return false;
      }
      // Rename each child in the src folder to destination/srcfolder/child
      String [] children = list(src);
      String parentName = getKeyName(getParentKey(src));
      for (String childKey: children) {
        if (!rename(childKey, CommonUtils.concatPath(dst, parentName))) {
          return false;
        }
      }
      // Delete everything under src
      return delete(src, true);
    }
    // Source is a file and Destination does not exist
    return copy(src, dst) && deleteInternal(src);
  }

  @Override
  public void setConf(Object conf) {

  }

  // Not supported
  public void setPermission(String path, String posixPerm) throws IOException {

  }

  /**
   * Treating S3 as a file system, checks if the parent directory exists.
   * @param key
   * @return
   */
  private boolean parentExists(String key) {
    // Root does not have a parent
    if (isRoot(key)) {
      return true;
    }
    String parentKey = getParentKey(key);
    return isFolder(parentKey);
  }

  /**
   * Appends the directory suffix to the key.
   * @param key
   * @return key as a directory path
   */
  private String convertToFolderName(String key) {
    return key + FOLDER_SUFFIX;
  }

  /**
   * Internal function to deleteInternal a key in S3
   * @param key the key to deleteInternal
   * @return true if successful, false if an exception is thrown
   * @throws IOException
   */
  private boolean deleteInternal(String key) throws IOException {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(key).substring(SCHEME.length()
            + mBucketName.length() + 1);
        mClient.deleteObject(mBucketName, keyAsFolder);
      } else {
        mClient.deleteObject(mBucketName, key.substring(SCHEME.length()
            + mBucketName.length() + 1));
      }
    } catch (ServiceException se) {
      LOG.error("Failed to delete " + key, se);
      return false;
    }
    return true;
  }

  private String getKeyName(String key) {
    int separatorIndex = key.lastIndexOf(PATH_SEPARATOR);
    if (separatorIndex >= 0) {
      return key.substring(separatorIndex, key.length());
    } else {
      return null;
    }
  }

  /**
   * Gets the StorageObject representing the metadata of a key. If the key does not exist as a
   * file or folder, null is returned
   * @param key
   * @return StorageObject of the key, or null if the key does not exist as a file or folder.
   */
  private StorageObject getObjectDetails(String key) {
    try {
      if (isFolder(key)) {
        String keyAsFolder = convertToFolderName(key).substring(SCHEME.length()
            + mBucketName.length() + 1);
        return mClient.getObjectDetails(mBucketName, keyAsFolder);
      } else {
        return mClient.getObjectDetails(mBucketName, key.substring(SCHEME.length()
            + mBucketName.length() + 1));
      }
    } catch (ServiceException se) {
      return null;
    }
  }

  private String getParentKey(String key) {
    int separatorIndex = key.lastIndexOf(PATH_SEPARATOR);
    if (separatorIndex >= 0) {
      return key.substring(0, separatorIndex);
    } else {
      return null;
    }
  }

  /**
   * Determines if the key is represents a folder. If false is returned, it is not guaranteed that
   * the path exists.
   * @param key
   * @return S3Object containing metadata
   * @throws IOException
   */
  private boolean isFolder(String key) {
    // Root is always a folder
    if (isRoot(key)) {
      return true;
    }
    try {
      String keyAsFolder = convertToFolderName(key).substring(SCHEME.length()
          + mBucketName.length() + 1);
      mClient.getObjectDetails(mBucketName, keyAsFolder);
      // If no exception is thrown, the key exists as a folder
      return true;
    } catch (ServiceException se) {
      return false;
    }
  }

  /**
   * Checks if the key is the root.
   * @param key
   * @return
   */
  private boolean isRoot(String key) {
    return key.equals(SCHEME + mBucketName);
  }

  /**
   * Creates a directory flagged file with the key and folder suffix.
   * @param key
   * @return
   */
  private boolean mkdir(String key) {
    try {
      String keyAsFolder =
          convertToFolderName(key).substring(SCHEME.length() + mBucketName.length() + 1);
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

  private boolean copy(String src, String dst) {
    try {
      src = src.substring(SCHEME.length() + mBucketName.length() + 1);
      dst = dst.substring(SCHEME.length() + mBucketName.length() + 1);
      LOG.info("Copying " + src + " to " + dst);
      S3Object obj = new S3Object(dst);
      mClient.copyObject(mBucketName, src, mBucketName, obj, false);
      return true;
    } catch (ServiceException se) {
      LOG.error("Failed to rename file " + src + " to " + dst);
      return false;
    }
  }

  private String[] listInternal(String path, boolean recursive) throws IOException {
    try {
      String separator = recursive ? "" : PATH_SEPARATOR;
      path = path.substring(SCHEME.length() + mBucketName.length() + 1);
      S3Object[] objs = mClient.listObjects(mBucketName, path, separator);
      String[] ret = new String[objs.length];
      for (int i = 0; i < objs.length; i ++) {
        ret[i] = objs[i].getKey().substring(path.length());
      }
      return ret;
    } catch (ServiceException se) {
      LOG.info("Failed to list path " + path);
      return null;
    }
  }

  /**
   * Transforms JetS3t's Service Exceptions to IOExceptions
   * @throws IOException
   */
  private void handleException(ServiceException se) throws IOException {
    if (se.getCause() instanceof IOException) {
      throw (IOException) se.getCause();
    } else {
      // Unexpected Service Exception
      Throwables.propagate(se);
    }
  }
}
