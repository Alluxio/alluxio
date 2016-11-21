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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A object based abstract {@link UnderFileSystem}.
 */
@ThreadSafe
public abstract class ObjectUnderFileSystem extends BaseUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Value used to indicate nested structure. */
  protected static final char PATH_SEPARATOR_CHAR = '/';

  /** Value used to indicate nested structure. */
  protected static final String PATH_SEPARATOR = String.valueOf(PATH_SEPARATOR_CHAR);

  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   */
  protected ObjectUnderFileSystem(AlluxioURI uri) {
    super(uri);
  }

  /**
   * A chunk of listing results.
   */
  public interface ObjectListingResult {
    /**
     * Objects in a pseudo-directory which may be a file or a directory.
     *
     * @return a list of objects
     */
    String[] getObjectNames();

    /**
     * Use common prefixes to infer pseudo-directories in object store.
     *
     * @return a list of common prefixes
     */
    String[] getCommonPrefixes();

    /**
     * Get next chunk of object listings.
     *
     * @return null if done with listing, otherwise return next chunk
     * @throws IOException
     */
    ObjectListingResult getNextChunk() throws IOException;
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
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return createDirect(path, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    if (mkdirs(getParentKey(path), true)) {
      return createOutputStream(path);
    }
    return null;
  }

  /**
   * Gets the block size in bytes. This method defaults to the default user block size in Alluxio.
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
    LOG.debug("getConf is not supported when using default ObjectUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using default ObjectUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.debug("getFileLocations is not supported when using default ObjectUnderFileSystem.");
    return null;
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  // Default object UFS does not provide a mechanism for updating the configuration, no-op
  @Override
  public void setConf(Object conf) {}

  @Override
  public boolean supportsFlush() {
    return false;
  }

  /**
   * Creates an {@link OutputStream} for object uploads.
   *
   * @param path ufs path including scheme and bucket
   * @throws IOException if failed to create stream
   * @return new OutputStream
   */
  protected abstract OutputStream createOutputStream(String path) throws IOException;

  /**
   * @param key ufs path including scheme and bucket
   * @return the parent key, or null if the parent does not exist
   */
  protected String getParentKey(String key) {
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
   * Checks if the key is the root.
   *
   * @param key ufs path including scheme and bucket
   * @return true if the key is the root, false otherwise
   */
  protected boolean isRoot(String key) {
    return PathUtils.normalizePath(key, PATH_SEPARATOR).equals(
        PathUtils.normalizePath(getRootKey(), PATH_SEPARATOR));
  }

  /**
   * Gets the child name based on the parent name.
   *
   * @param child the key of the child
   * @param parent the key of the parent
   * @return the child key with the parent prefix removed, null if the parent prefix is invalid
   */
  protected String getChildName(String child, String parent) {
    if (child.startsWith(parent)) {
      return child.substring(parent.length());
    }
    LOG.error("Attempted to get childname with an invalid parent argument. Parent: {} Child: {}",
        parent, child);
    return null;
  }

  /**
   * Get suffix used to encode a directory.
   *
   * @return folder suffix
   */
  protected abstract String getFolderSuffix();

  /**
   * Get object listing for given path.
   *
   * @param path pseudo-directory path excluding header and bucket
   * @param recursive whether to request immediate children only, or all descendants
   * @return chunked object listing
   * @throws IOException if a non-alluxio error occurs
   */
  protected abstract ObjectListingResult getObjectListing(String path, boolean recursive)
      throws IOException;

  /**
   * Get full path of root in object store.
   *
   * @return full path including scheme and bucket
   */
  protected abstract String getRootKey();

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix. Note that, the list results are unsorted.
   *
   * @param path the key to list
   * @param recursive if true will list children directories as well
   * @return an array of the file and folder names in this directory
   * @throws IOException if an I/O error occurs
   */
  protected UnderFileStatus[] listInternal(String path, boolean recursive) throws IOException {
    path = stripPrefixIfPresent(path);
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    path = path.equals(PATH_SEPARATOR) ? "" : path;
    Map<String, Boolean> children = new HashMap<>();
    ObjectListingResult chunk = getObjectListing(path, recursive);
    chunk = chunk.getNextChunk();
    if (chunk == null) {
      return null;
    }
    while (chunk != null) {
      // Directories in UFS can be possibly encoded in two different ways:
      // (1) as file objects with FOLDER_SUFFIX for directories created through Alluxio or
      // (2) as "common prefixes" of other files objects for directories not created through
      // Alluxio
      //
      // Case (1) (and file objects) is accounted for by iterating over chunk.getObjects() while
      // case (2) is accounted for by iterating over chunk.getCommonPrefixes().
      //
      // An example, with prefix="ufs" and delimiter="/" and LISTING_LENGTH=5
      // - objects.key = ufs/, child =
      // - objects.key = ufs/dir1<FOLDER_SUFFIX>, child = dir1
      // - objects.key = ufs/file, child = file
      // - commonPrefix = ufs/dir1/, child = dir1
      // - commonPrefix = ufs/dir2/, child = dir2

      // Handle case (1)
      for (String obj : chunk.getObjectNames()) {
        // Remove parent portion of the key
        String child = getChildName(obj, path);
        // Prune the special folder suffix
        boolean isDir = child.endsWith(getFolderSuffix());
        child = CommonUtils.stripSuffixIfPresent(child, getFolderSuffix());
        // Only add if the path is not empty (removes results equal to the path)
        if (!child.isEmpty()) {
          children.put(child, isDir);
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
          if (!child.isEmpty() && !children.containsKey(child)) {
            // This directory has not been created through Alluxio.
            mkdirsInternal(commonPrefix);
            // If both a file and a directory existed with the same name, the path will be
            // treated as a directory
            children.put(child, true);
          }
        }
      }
      chunk = chunk.getNextChunk();
    }
    UnderFileStatus[] ret = new UnderFileStatus[children.size()];
    int pos = 0;
    for (Map.Entry<String, Boolean> entry : children.entrySet()) {
      ret[pos++] = new UnderFileStatus(entry.getKey(), entry.getValue());
    }
    return ret;
  }

  /**
   * Creates a directory flagged file with the key and folder suffix.
   *
   * @param key the key to create a folder
   * @return true if the operation was successful, false otherwise
   */
  protected abstract boolean mkdirsInternal(String key);

  /**
   * Treating the object store as a file system, checks if the parent directory exists.
   *
   * @param key the key to check
   * @return true if the parent exists or if the key is root, false otherwise
   */
  protected boolean parentExists(String key) throws IOException {
    // Assume root always has a parent
    if (isRoot(key)) {
      return true;
    }
    String parentKey = getParentKey(key);
    return parentKey != null && isDirectory(parentKey);
  }

  /**
   * Strips the bucket prefix or the preceding path separator from the key if it is present. For
   * example, for input key ufs://my-bucket-name/my-path/file, the output would be my-path/file. If
   * key is an absolute path like /my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param key the key to strip
   * @return the key without the bucket prefix
   */
  protected String stripPrefixIfPresent(String key) {
    String stripedKey = CommonUtils.stripPrefixIfPresent(key, getRootKey());
    if (!stripedKey.equals(key)) {
      return stripedKey;
    }
    return CommonUtils.stripPrefixIfPresent(key, PATH_SEPARATOR);
  }
}
