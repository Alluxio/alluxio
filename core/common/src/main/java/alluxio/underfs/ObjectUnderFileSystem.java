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
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An object based abstract {@link UnderFileSystem}. Object Stores implementing the
 * {@link UnderFileSystem} interface should derive from this class.
 */
@ThreadSafe
public abstract class ObjectUnderFileSystem extends BaseUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectUnderFileSystem.class);

  /** Default maximum length for a single listing query. */
  private static final int DEFAULT_MAX_LISTING_CHUNK_LENGTH = 1000;

  /** Value used to indicate nested structure. */
  protected static final char PATH_SEPARATOR_CHAR = '/';

  /**
   * Value used to indicate nested structure. This is a string representation of
   * {@link ObjectUnderFileSystem#PATH_SEPARATOR_CHAR}.
   */
  protected static final String PATH_SEPARATOR = String.valueOf(PATH_SEPARATOR_CHAR);

  /** Executor service used for parallel UFS operations such as bulk deletes. */
  protected ExecutorService mExecutorService;

  /**
   * Constructs an {@link ObjectUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   * @param ufsConf UFS configuration
   */
  protected ObjectUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);

    int numThreads = Configuration.getInt(PropertyKey.UNDERFS_OBJECT_STORE_SERVICE_THREADS);
    mExecutorService = ExecutorServiceFactories.fixedThreadPoolExecutorServiceFactory(
        "alluxio-underfs-object-service-worker", numThreads).create();
  }

  /**
   * Information about a single object in object UFS.
   */
  protected class ObjectStatus {
    private static final long INVALID_CONTENT_LENGTH = -1L;
    private static final long INVALID_MODIFIED_TIME = -1L;

    private final long mContentLength;
    private final long mLastModifiedTimeMs;
    private final String mName;

    public ObjectStatus(String name, long contentLength, long lastModifiedTimeMs) {
      mContentLength = contentLength;
      mLastModifiedTimeMs = lastModifiedTimeMs;
      mName = name;
    }

    public ObjectStatus(String name) {
      mContentLength = INVALID_CONTENT_LENGTH;
      mLastModifiedTimeMs = INVALID_MODIFIED_TIME;
      mName = name;
    }

    /**
     * Gets the size of object contents in bytes.
     *
     * @return the content length in bytes
     */
    public long getContentLength() {
      return mContentLength;
    }

    /**
     * Gets the last modified epoch time in ms.
     *
     * @return modification time in milliseconds
     */
    public long getLastModifiedTimeMs() {
      return mLastModifiedTimeMs;
    }

    /**
     * Gets the name of the object.
     *
     * @return object name
     */
    public String getName() {
      return mName;
    }
  }

  /**
   * A chunk of listing results.
   */
  public interface ObjectListingChunk {
    /**
     * Objects in a pseudo-directory which may be a file or a directory.
     *
     * @return a list of object statuses
     */
    ObjectStatus[] getObjectStatuses();

    /**
     * Use common prefixes to infer pseudo-directories in object store.
     *
     * @return a list of common prefixes
     */
    String[] getCommonPrefixes();

    /**
     * Get next chunk of object listings.
     *
     * @return null if listing did not find anything or is done, otherwise return new
     * {@link ObjectListingChunk} for the next chunk
     */
    ObjectListingChunk getNextChunk() throws IOException;
  }

  /**
   * Permissions in object UFS.
   */
  public class ObjectPermissions {
    final String mOwner;
    final String mGroup;
    final short mMode;

    /**
     * Creates a new ObjectPermissions.
     *
     * @param owner the owner of the object
     * @param group the group of the object
     * @param mode the representation of the permission bits
     */
    public ObjectPermissions(String owner, String group, short mode) {
      mOwner = owner;
      mGroup = group;
      mMode = mode;
    }

    /**
     * @return the name of the owner
     */
    public String getOwner() {
      return mOwner;
    }

    /**
     * @return the name of the group
     */
    public String getGroup() {
      return mGroup;
    }

    /**
     * @return the short representation of the permission bits
     */
    public short getMode() {
      return mMode;
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(String hostname) {
  }

  @Override
  public void connectFromWorker(String hostname) {
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (options.getCreateParent() && !mkdirs(getParentPath(path))) {
      throw new IOException(ExceptionMessage.PARENT_CREATION_FAILED.getMessage(path));
    }
    return createObject(stripPrefixIfPresent(path));
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return deleteObject(stripPrefixIfPresent(path));
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    if (!options.isRecursive()) {
      UfsStatus[] children = listInternal(path, ListOptions.defaults());
      if (children == null) {
        LOG.error("Unable to delete path because {} is not a directory ", path);
        return false;
      }
      if (children.length != 0) {
        LOG.error("Unable to delete {} because it is a non empty directory. Specify "
                + "recursive as true in order to delete non empty directories.", path);
        return false;
      }
      // Delete the directory itself
      return deleteObject(stripPrefixIfPresent(convertToFolderName(path)));
    }

    // Delete children
    DeleteBuffer deleteBuffer = new DeleteBuffer();
    UfsStatus[] pathsToDelete = listInternal(path, ListOptions.defaults().setRecursive(true));
    if (pathsToDelete == null) {
      LOG.warn("Unable to delete {} because listInternal returns null", path);
      return false;
    }
    for (UfsStatus pathToDelete : pathsToDelete) {
      String pathKey = stripPrefixIfPresent(PathUtils.concatPath(path, pathToDelete.getName()));
      if (pathToDelete.isDirectory()) {
        deleteBuffer.add(convertToFolderName(pathKey));
      } else {
        deleteBuffer.add(pathKey);
      }
    }
    deleteBuffer.add(stripPrefixIfPresent(convertToFolderName(path)));
    int filesDeleted = deleteBuffer.getResult().size();
    if (filesDeleted != deleteBuffer.mEntriesAdded) {
      LOG.warn("Failed to delete directory, successfully deleted {} files out of {}.",
          filesDeleted, deleteBuffer.mEntriesAdded);
      return false;
    }
    return true;
  }

  /**
   * Objects added to a {@link DeleteBuffer} will be deleted in batches. Multiple batches are
   * processed in parallel.
   */
  @NotThreadSafe
  protected class DeleteBuffer {
    /** A list of objects in batches to be deleted in parallel. */
    private ArrayList<List<String>> mBatches;
    /** A list of the successfully deleted objects for each batch delete. */
    private ArrayList<Future<List<String>>> mBatchesResult;
    /** Buffer for a batch of objects to be deleted. */
    private List<String> mCurrentBatchBuffer;
    /** Total number of objects to be deleted across batches. */
    private int mEntriesAdded;

    /**
     * Construct a new {@link DeleteBuffer} instance.
     */
    public DeleteBuffer() {
      mBatches = new ArrayList<>();
      mBatchesResult = new ArrayList<>();
      mCurrentBatchBuffer = new LinkedList<>();
      mEntriesAdded = 0;
    }

    /**
     * Add a new object to be deleted.
     *
     * @param path of object
     * @throws IOException if a non-Alluxio error occurs
     */
    public void add(String path) throws IOException {
      // Delete batch size is same as listing length
      if (mCurrentBatchBuffer.size() == getListingChunkLength()) {
        // Batch is full
        submitBatch();
      }
      mCurrentBatchBuffer.add(path);
      mEntriesAdded++;
    }

    /**
     * Get the combined result from all batches.
     *
     * @return a list of successfully deleted objects
     * @throws IOException if a non-Alluxio error occurs
     */
    public List<String> getResult() throws IOException {
      submitBatch();
      LinkedList<String> result = new LinkedList<>();
      for (Future<List<String>> list : mBatchesResult) {
        try {
          result.addAll(list.get());
        } catch (InterruptedException e) {
          // If operation was interrupted do not add to successfully deleted list
          LOG.warn("Interrupted while waiting for the result of batch delete. UFS and Alluxio "
              + "state may be inconsistent. Error: {}", e.getMessage());
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          // If operation failed to execute do not add to successfully deleted list
          LOG.warn("A batch delete failed. UFS and Alluxio state may be inconsistent. Error: {}",
              e.getMessage());
        }
      }
      return result;
    }

    /**
     * Process the current batch asynchronously.
     */
    private void submitBatch() throws IOException {
      if (mCurrentBatchBuffer.size() != 0) {
        int batchNumber = mBatches.size();
        mBatches.add(new LinkedList<>(mCurrentBatchBuffer));
        mCurrentBatchBuffer.clear();
        mBatchesResult.add(batchNumber,
            mExecutorService.submit(new DeleteThread(mBatches.get(batchNumber))));
      }
    }

    /**
     * Thread class to delete a batch of objects.
     */
    @NotThreadSafe
    protected class DeleteThread implements Callable<List<String>> {

      List<String> mBatch;

      /**
       * Delete a batch of objects.
       * @param batch a list of objects to delete
       */
      public DeleteThread(List<String> batch) {
        mBatch = batch;
      }

      @Override
      public List<String> call() {
        try {
          return deleteObjects(mBatch);
        } catch (IOException e) {
          // Do not append to success list
          return Collections.emptyList();
        }
      }
    }
  }

  /**
   * Gets the block size in bytes. This method defaults to the default user block size in Alluxio.
   *
   * @param path the file name
   * @return the default Alluxio user block size
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    if (isDirectory(path)) {
      ObjectPermissions permissions = getPermissions();
      return new UfsDirectoryStatus(path, permissions.getOwner(), permissions.getGroup(),
          permissions.getMode());
    }
    LOG.warn("Error fetching directory status, assuming directory {} does not exist", path);
    throw new FileNotFoundException(path);
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using default ObjectUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    LOG.debug("getFileLocations is not supported when using default ObjectUnderFileSystem.");
    return null;
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    ObjectStatus details = getObjectStatus(stripPrefixIfPresent(path));
    if (details != null) {
      ObjectPermissions permissions = getPermissions();
      return new UfsFileStatus(path, details.getContentLength(), details.getLastModifiedTimeMs(),
          permissions.getOwner(), permissions.getGroup(), permissions.getMode());
    } else {
      LOG.warn("Error fetching file status, assuming file {} does not exist", path);
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    // Root is always a folder
    if (isRoot(path)) {
      return true;
    }
    String keyAsFolder = convertToFolderName(stripPrefixIfPresent(path));
    if (getObjectStatus(keyAsFolder) != null) {
      return true;
    }
    return getObjectListingChunkForPath(path, true) != null;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    // Directly try to get the file metadata, if we fail it either is a folder or does not exist
    return !isRoot(path) && (getObjectStatus(stripPrefixIfPresent(path)) != null);
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return listInternal(path, ListOptions.defaults());
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options)
      throws IOException {
    return listInternal(path, options);
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
      String parentKey = getParentPath(path);
      // Recursively make the parent folders
      return mkdirs(parentKey) && mkdirsInternal(path);
    }
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return openObject(stripPrefixIfPresent(path), options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    UfsStatus[] children = listInternal(src, ListOptions.defaults());
    if (children == null) {
      LOG.error("Failed to list directory {}, aborting rename.", src);
      return false;
    }
    if (exists(dst)) {
      LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
      return false;
    }
    // Source exists and is a directory, and destination does not exist
    // Rename the source folder first
    if (!copyObject(stripPrefixIfPresent(convertToFolderName(src)),
        stripPrefixIfPresent(convertToFolderName(dst)))) {
      return false;
    }
    // Rename each child in the src folder to destination/child
    for (UfsStatus child : children) {
      String childSrcPath = PathUtils.concatPath(src, child);
      String childDstPath = PathUtils.concatPath(dst, child);
      boolean success;
      if (child.isDirectory()) {
        // Recursive call
        success = renameDirectory(childSrcPath, childDstPath);
      } else {
        success = renameFile(childSrcPath, childDstPath);
      }
      if (!success) {
        LOG.error("Failed to rename path {}, aborting rename.", child);
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
    if (exists(dst)) {
      LOG.error("Unable to rename {} to {} because destination already exists.", src, dst);
      return false;
    }
    // Source is a file and Destination does not exist
    return copyObject(stripPrefixIfPresent(src), stripPrefixIfPresent(dst))
        && deleteObject(stripPrefixIfPresent(src));
  }

  @Override
  public boolean supportsFlush() {
    return false;
  }

  /**
   * Create a zero-byte object used to encode a directory.
   *
   * @param key the key to create
   * @return true if the operation was successful
   */
  protected abstract boolean createEmptyObject(String key);

  /**
   * Creates an {@link OutputStream} for object uploads.
   *
   * @param key ufs key including scheme and bucket
   * @return new OutputStream
   */
  protected abstract OutputStream createObject(String key) throws IOException;

  /**
   * Appends the directory suffix to the key.
   *
   * @param key the key to convert
   * @return key as a directory path
   */
  protected String convertToFolderName(String key) {
    // Strips the slash if it is the end of the key string. This is because the slash at
    // the end of the string is not part of the Object key.
    key = CommonUtils.stripSuffixIfPresent(key, PATH_SEPARATOR);
    return key + getFolderSuffix();
  }

  /**
   * Copies an object to another key.
   *
   * @param src the source key to copy
   * @param dst the destination key to copy to
   * @return true if the operation was successful, false otherwise
   */
  protected abstract boolean copyObject(String src, String dst) throws IOException;

  /**
   * Internal function to delete a key.
   *
   * @param key the key to delete
   * @return true if successful, false if an exception is thrown
   */
  protected abstract boolean deleteObject(String key) throws IOException;

  /**
   * Internal function to delete a list of keys.
   *
   * @param keys the list of keys to delete
   * @return list of successfully deleted keys
   */
  protected List<String> deleteObjects(List<String> keys) throws IOException {
    List<String> result = new LinkedList<>();
    for (String key : keys) {
      boolean status = deleteObject(key);
      // If key is a directory, it is possible that it was not created through Alluxio and no
      // zero-byte breadcrumb exists
      if (status || key.endsWith(getFolderSuffix())) {
        result.add(key);
      }
    }
    return result;
  }

  /**
   * Permissions for the mounted bucket.
   *
   * @return permissions
   */
  protected abstract ObjectPermissions getPermissions();

  /**
   * Maximum number of items in a single listing chunk supported by the under store.
   *
   * @return the maximum length for a single listing query
   */
  protected int getListingChunkLengthMax() {
    return DEFAULT_MAX_LISTING_CHUNK_LENGTH;
  }

  /**
   * The number of items to query in a single listing chunk.
   *
   * @return length of each list request
   */
  protected int getListingChunkLength() {
    return Configuration.getInt(PropertyKey.UNDERFS_LISTING_LENGTH) > getListingChunkLengthMax()
        ? getListingChunkLengthMax() : Configuration.getInt(PropertyKey.UNDERFS_LISTING_LENGTH);
  }

  /**
   * Get metadata information about object. Implementations should process the key as is, which
   * may be a file or a directory key.
   *
   * @param key ufs key to get metadata for
   * @return {@link ObjectStatus} if key exists and successful, otherwise null
   */
  protected abstract ObjectStatus getObjectStatus(String key);

  /**
   * Get parent path.
   *
   * @param path ufs path including scheme and bucket
   * @return the parent path, or null if the parent does not exist
   */
  protected String getParentPath(String path) {
    // Root does not have a parent.
    if (isRoot(path)) {
      return null;
    }
    int separatorIndex = path.lastIndexOf(PATH_SEPARATOR);
    if (separatorIndex < 0) {
      return null;
    }
    return path.substring(0, separatorIndex);
  }

  /**
   * Checks if the path is the root.
   *
   * @param path ufs path including scheme and bucket
   * @return true if the path is the root, false otherwise
   */
  protected boolean isRoot(String path) {
    return PathUtils.normalizePath(path, PATH_SEPARATOR).equals(
        PathUtils.normalizePath(getRootKey(), PATH_SEPARATOR));
  }

  /**
   * Gets the child name based on the parent name.
   *
   * @param child the key of the child
   * @param parent the key of the parent
   * @return the child key with the parent prefix removed
   */
  protected String getChildName(String child, String parent) throws IOException {
    if (child.startsWith(parent)) {
      return child.substring(parent.length());
    }
    throw new IOException(ExceptionMessage.INVALID_PREFIX.getMessage(parent, child));
  }

  /**
   * Get suffix used to encode a directory.
   *
   * @return folder suffix
   */
  protected abstract String getFolderSuffix();

  /**
   * Gets a (partial) object listing result for the given key.
   *
   * @param key pseudo-directory key excluding header and bucket
   * @param recursive whether to request immediate children only, or all descendants
   * @return chunked object listing, or null if key is not found
   */
  protected abstract ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException;

  /**
   * Gets a (partial) object listing for the given path.
   *
   * @param path of pseudo-directory
   * @param recursive whether to request immediate children only, or all descendants
   * @return chunked object listing, or null if the path does not exist as a pseudo-directory
   */
  protected ObjectListingChunk getObjectListingChunkForPath(String path, boolean recursive)
      throws IOException {
    // Check if anything begins with <folder_path>/
    String dir = stripPrefixIfPresent(path);
    ObjectListingChunk objs = getObjectListingChunk(dir, recursive);
    // If there are, this is a folder and we can create the necessary metadata
    if (objs != null && ((objs.getObjectStatuses() != null && objs.getObjectStatuses().length > 0)
        || (objs.getCommonPrefixes() != null && objs.getCommonPrefixes().length > 0))) {
      // If the breadcrumb exists, this is a no-op
      if (!mUfsConf.isReadOnly()) {
        mkdirsInternal(dir);
      }
      return objs;
    }
    return null;
  }

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
   * @param options for listing
   * @return an array of the file and folder names in this directory
   */
  protected UfsStatus[] listInternal(String path, ListOptions options) throws IOException {
    ObjectListingChunk chunk = getObjectListingChunkForPath(path, options.isRecursive());
    if (chunk == null) {
      String keyAsFolder = convertToFolderName(stripPrefixIfPresent(path));
      if (getObjectStatus(keyAsFolder) != null) {
        // Path is an empty directory
        return new UfsStatus[0];
      }
      return null;
    }
    String keyPrefix = PathUtils.normalizePath(stripPrefixIfPresent(path), PATH_SEPARATOR);
    keyPrefix = keyPrefix.equals(PATH_SEPARATOR) ? "" : keyPrefix;
    Map<String, UfsStatus> children = new HashMap<>();
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
      for (ObjectStatus status : chunk.getObjectStatuses()) {
        // Remove parent portion of the key
        String child = getChildName(status.getName(), keyPrefix);
        if (child.isEmpty() || child.equals(getFolderSuffix())) {
          // Removes results equal to the path
          continue;
        }
        ObjectPermissions permissions = getPermissions();
        if (child.endsWith(getFolderSuffix())) {
          // Child is a directory
          child = CommonUtils.stripSuffixIfPresent(child, getFolderSuffix());
          children.put(child, new UfsDirectoryStatus(child, permissions.getOwner(),
              permissions.getGroup(), permissions.getMode()));
        } else {
          // Child is a file
          children.put(child,
              new UfsFileStatus(child, status.getContentLength(), status.getLastModifiedTimeMs(),
                  permissions.getOwner(), permissions.getGroup(), permissions.getMode()));
        }
      }
      // Handle case (2)
      String[] commonPrefixes;
      if (options.isRecursive()) {
        // In case of a recursive listing infer pseudo-directories as the commonPrefixes returned
        // from the object store is empty for an empty delimiter.
        HashSet<String> prefixes = new HashSet<>();
        for (ObjectStatus objectStatus : chunk.getObjectStatuses()) {
          String objectName = objectStatus.getName();
          while (objectName.startsWith(keyPrefix)) {
            objectName = objectName.substring(0, objectName.lastIndexOf(PATH_SEPARATOR));
            if (!objectName.isEmpty()) {
              prefixes.add(objectName);
            }
          }
        }
        commonPrefixes = prefixes.toArray(new String[prefixes.size()]);
      } else {
        commonPrefixes = chunk.getCommonPrefixes();
      }
      for (String commonPrefix : commonPrefixes) {
        if (commonPrefix.startsWith(keyPrefix)) {
          // Remove parent portion of the key
          String child = getChildName(commonPrefix, keyPrefix);
          // Remove any portion after the last path delimiter
          int childNameIndex = child.lastIndexOf(PATH_SEPARATOR);
          child = childNameIndex != -1 ? child.substring(0, childNameIndex) : child;
          if (!child.isEmpty() && !children.containsKey(child)) {
            // This directory has not been created through Alluxio.
            if (!mUfsConf.isReadOnly()) {
              mkdirsInternal(commonPrefix);
            }
            // If both a file and a directory existed with the same name, the path will be
            // treated as a directory
            ObjectPermissions permissions = getPermissions();
            children.put(child, new UfsDirectoryStatus(child, permissions.getOwner(),
                permissions.getGroup(), permissions.getMode()));
          }
        }
      }
      chunk = chunk.getNextChunk();
    }
    UfsStatus[] ret = new UfsStatus[children.size()];
    int pos = 0;
    for (UfsStatus status : children.values()) {
      ret[pos++] = status;
    }
    return ret;
  }

  /**
   * Creates a directory flagged file with the key and folder suffix.
   *
   * @param key the key to create a folder
   * @return true if the operation was successful, false otherwise
   */
  protected boolean mkdirsInternal(String key) {
    return createEmptyObject(convertToFolderName(stripPrefixIfPresent(key)));
  }

  /**
   * Internal function to open an input stream to an object.
   *
   * @param key the key to open
   * @return an {@link InputStream} to read from key
   */
  protected abstract InputStream openObject(String key, OpenOptions options) throws IOException;

  /**
   * Treating the object store as a file system, checks if the parent directory exists.
   *
   * @param path the path to check
   * @return true if the parent exists or if the path is root, false otherwise
   */
  protected boolean parentExists(String path) throws IOException {
    // Assume root always has a parent
    if (isRoot(path)) {
      return true;
    }
    String parentKey = getParentPath(path);
    return parentKey != null && isDirectory(parentKey);
  }

  /**
   * Strips the bucket prefix or the preceding path separator from the path if it is present. For
   * example, for input path ufs://my-bucket-name/my-path/file, the output would be my-path/file. If
   * path is an absolute path like /my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param path the path to strip
   * @return the path without the bucket prefix
   */
  private String stripPrefixIfPresent(String path) {
    String stripedKey = CommonUtils.stripPrefixIfPresent(path,
        PathUtils.normalizePath(getRootKey(), PATH_SEPARATOR));
    if (!stripedKey.equals(path)) {
      return stripedKey;
    }
    return CommonUtils.stripPrefixIfPresent(path, PATH_SEPARATOR);
  }
}
