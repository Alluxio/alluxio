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

package alluxio.underfs.s3a;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.util.Base64;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
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
 * S3 {@link alluxio.underfs.UnderFileSystem} implementation based on the aws-java-sdk-s3 library.
 */
@ThreadSafe
public class S3AUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Value used to indicate folder structure in S3. */
  private static final String PATH_SEPARATOR = "/";

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH;

  /** Length of each list request in S3. */
  private static final int LISTING_LENGTH = 1000;

  /** Threshold to do multipart copy. */
  private static final long MULTIPART_COPY_THRESHOLD = 100 * Constants.MB;

  /** AWS-SDK S3 client. */
  private final AmazonS3Client mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** Prefix of the bucket, for example s3a://my-bucket-name/. */
  private final String mBucketPrefix;

  /** Transfer Manager for efficient I/O to s3. */
  private final TransferManager mManager;

  static {
    byte[] dirByteHash = DigestUtils.md5(new byte[0]);
    DIR_HASH = new String(Base64.encode(dirByteHash));
  }

  /**
   * Constructs a new instance of {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public S3AUnderFileSystem(AlluxioURI uri) {
    super(uri);
    mBucketName = uri.getHost();
    mBucketPrefix = PathUtils.normalizePath(Constants.HEADER_S3A + mBucketName, PATH_SEPARATOR);

    // Set the aws credential system properties based on Alluxio properties, if they are set
    if (Configuration.containsKey(Constants.S3A_ACCESS_KEY)) {
      System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY,
          Configuration.get(Constants.S3A_ACCESS_KEY));
    }
    if (Configuration.containsKey(Constants.S3A_SECRET_KEY)) {
      System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY,
          Configuration.get(Constants.S3A_SECRET_KEY));
    }

    // Checks, in order, env variables, system properties, profile file, and instance profile
    AWSCredentialsProvider credentials =
        new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain());

    // Set the client configuration based on Alluxio configuration values
    ClientConfiguration clientConf = new ClientConfiguration();

    // Socket timeout
    clientConf.setSocketTimeout(Configuration.getInt(Constants.UNDERFS_S3A_SOCKET_TIMEOUT_MS));

    // HTTP protocol
    if (Configuration.getBoolean(Constants.UNDERFS_S3A_SECURE_HTTP_ENABLED)) {
      clientConf.setProtocol(Protocol.HTTPS);
    } else {
      clientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (Configuration.containsKey(Constants.UNDERFS_S3_PROXY_HOST)) {
      clientConf.setProxyHost(Configuration.get(Constants.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (Configuration.containsKey(Constants.UNDERFS_S3_PROXY_PORT)) {
      clientConf.setProxyPort(Configuration.getInt(Constants.UNDERFS_S3_PROXY_PORT));
    }

    mClient = new AmazonS3Client(credentials, clientConf);
    if (Configuration.containsKey(Constants.UNDERFS_S3_ENDPOINT)) {
      mClient.setEndpoint(Configuration.get(Constants.UNDERFS_S3_ENDPOINT));
    }
    mManager = new TransferManager(mClient);

    TransferManagerConfiguration transferConf = new TransferManagerConfiguration();
    transferConf.setMultipartCopyThreshold(MULTIPART_COPY_THRESHOLD);
    mManager.setConfiguration(transferConf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.S3;
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
      return new S3AOutputStream(mBucketName, stripPrefixIfPresent(path), mManager);
    }
    return null;
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
    return Configuration.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
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
    ObjectMetadata details = getObjectDetails(path);
    if (details != null) {
      return details.getContentLength();
    } else {
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    ObjectMetadata details = getObjectDetails(path);
    if (details != null) {
      return details.getLastModified().getTime();
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
    // Directly try to get the file metadata, if we fail it either is a folder or does not exist
    try {
      mClient.getObjectMetadata(mBucketName, stripPrefixIfPresent(path));
      return true;
    } catch (AmazonClientException e) {
      return false;
    }
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
      return new S3AInputStream(mBucketName, path, mClient);
    } catch (AmazonClientException e) {
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
   * @throws java.io.IOException if failed to open file at position
   */
  public InputStream openAtPosition(String path, long pos) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      return new S3AInputStream(mBucketName, path, mClient, pos);
    } catch (AmazonClientException e) {
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

  // This UFS does not provide a mechanism for updating the configuration, no-op
  @Override
  public void setConf(Object conf) {}

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // No ACL integration currently, returns default empty value
  @Override
  public String getOwner(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default empty value
  @Override
  public String getGroup(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default value
  @Override
  public short getMode(String path) throws IOException {
    return Constants.DEFAULT_FILE_SYSTEM_MODE;
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
    key = CommonUtils.stripSuffixIfPresent(key, PATH_SEPARATOR);
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
    // Retry copy for a few times, in case some AWS internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {
        CopyObjectRequest request = new CopyObjectRequest(mBucketName, src, mBucketName, dst);
        if (Configuration.getBoolean(Constants.UNDERFS_S3A_SERVER_SIDE_ENCRYPTION_ENABLED)) {
          ObjectMetadata meta = new ObjectMetadata();
          meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
          request.setNewObjectMetadata(meta);
        }
        mManager.copy(request).waitForCopyResult();
        return true;
      } catch (AmazonClientException | InterruptedException e) {
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
    } catch (AmazonClientException e) {
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
   * Gets the metadata associated with a non-root key if it represents a folder. This method will
   * return null if the key is not a folder. If the key exists as a prefix but does not have the
   * folder dummy file, a folder dummy file will be created.
   *
   * @param key the key to get the metadata for
   * @return the metadata of the folder, or null if the key does not represent a folder
   */
  private ObjectMetadata getFolderMetadata(String key) {
    Preconditions.checkArgument(!isRoot(key));
    String keyAsFolder = convertToFolderName(stripPrefixIfPresent(key));
    ObjectMetadata meta = null;
    try {
      meta = mClient.getObjectMetadata(mBucketName, keyAsFolder);
      // If no exception is thrown, the key exists as a folder
    } catch (AmazonClientException e) {
      // It is possible that the folder has not been encoded as a _$folder$ file
      try {
        String dir = stripPrefixIfPresent(key);
        String dirPrefix = PathUtils.normalizePath(dir, PATH_SEPARATOR);
        // Check if anything begins with <folder_path>/
        ObjectListing objs = mClient.listObjects(mBucketName, dirPrefix);
        // If there are, this is a folder and we can create the necessary metadata
        if (objs.getObjectSummaries().size() > 0) {
          mkdirsInternal(dir);
          meta = mClient.getObjectMetadata(mBucketName, keyAsFolder);
        }
      } catch (AmazonClientException ace) {
        return null;
      }
    }
    return meta;
  }

  /**
   * @param key the key to get the object details of
   * @return {@link ObjectMetadata} of the key, or null if the key does not exist
   */
  private ObjectMetadata getObjectDetails(String key) {
    // We try to get the metadata as a file and then a folder without checking isFolder to reduce
    // the number of calls to S3.
    try {
      return mClient.getObjectMetadata(mBucketName, stripPrefixIfPresent(key));
    } catch (AmazonClientException e) {
      // Its possible that the object is not a file but a folder
      return getFolderMetadata(stripPrefixIfPresent(key));
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
    return isRoot(key) || getFolderMetadata(key) != null;
  }

  /**
   * Checks if the key is the root.
   *
   * @param key the key to check
   * @return true if the key is the root, false otherwise
   */
  private boolean isRoot(String key) {
    return PathUtils.normalizePath(key, PATH_SEPARATOR).equals(
        PathUtils.normalizePath(Constants.HEADER_S3A + mBucketName, PATH_SEPARATOR));
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
    Set<String> children = new HashSet<>();
    try {
      ListObjectsV2Request request =
          new ListObjectsV2Request().withBucketName(mBucketName).withPrefix(path)
              .withDelimiter(delimiter).withMaxKeys(LISTING_LENGTH);
      ListObjectsV2Result result = null;
      while (result == null || result.isTruncated()) {
        // Query S3 for the next batch of objects
        result = mClient.listObjectsV2(request);
        // Advance the request continuation token to the next set of objects
        request.setContinuationToken(result.getNextContinuationToken());

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

        // Handle case (1)
        for (S3ObjectSummary obj : result.getObjectSummaries()) {
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
        for (String commonPrefix : result.getCommonPrefixes()) {
          // Remove parent portion of the key
          String child = getChildName(commonPrefix, path);
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
      return children.toArray(new String[children.size()]);
    } catch (AmazonClientException e) {
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
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(0);
      meta.setContentMD5(DIR_HASH);
      meta.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);
      mClient.putObject(new PutObjectRequest(mBucketName, keyAsFolder, new ByteArrayInputStream(
          new byte[0]), meta));
      return true;
    } catch (AmazonClientException e) {
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
   * example, for input key s3a://my-bucket-name/my-path/file, the output would be my-path/file. If
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
