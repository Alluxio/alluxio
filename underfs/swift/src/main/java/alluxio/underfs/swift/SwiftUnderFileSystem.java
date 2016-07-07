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

package alluxio.underfs.swift;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.swift.http.SwiftDirectClient;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.PaginationMap;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OpenStack Swift {@link UnderFileSystem} implementation based on the JOSS library.
 * The mkdir operation creates a zero-byte object.
 * A suffix {@link SwiftUnderFileSystem#PATH_SEPARATOR} in the object name denotes a folder.
 */
// TODO(adit): Abstract out functionality common with other object under storage systems.
@ThreadSafe
public class SwiftUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Value used to indicate nested structure in Swift. */
  private static final char PATH_SEPARATOR_CHAR = '/';

  /** Value used to indicate nested structure in Swift. */
  private static final String PATH_SEPARATOR = String.valueOf(PATH_SEPARATOR_CHAR);

  /** Maximum number of directory entries to fetch at once. */
  private static final int DIR_PAGE_SIZE = 100;

  /** Number of retries in case of Swift internal errors. */
  private static final int NUM_RETRIES = 3;

  /** Swift account. */
  private final Account mAccount;

  /** Container name of user's configured Alluxio container. */
  private final String mContainerName;

  /** Prefix of the container, for example swift://my-container-name/ . */
  private final String mContainerPrefix;

  /** JOSS access object. */
  private final Access mAccess;

  /**
   * Constructs a new Swift {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public SwiftUnderFileSystem(AlluxioURI uri) {
    super(uri);
    String containerName = uri.getHost();
    LOG.debug("Constructor init: {}", containerName);
    AccountConfig config = new AccountConfig();
    if (Configuration.containsKey(Constants.SWIFT_API_KEY)) {
      config.setPassword(Configuration.get(Constants.SWIFT_API_KEY));
    } else if (Configuration.containsKey(Constants.SWIFT_PASSWORD_KEY)) {
      config.setPassword(Configuration.get(Constants.SWIFT_PASSWORD_KEY));
    }
    config.setAuthUrl(Configuration.get(Constants.SWIFT_AUTH_URL_KEY));
    String authMethod = Configuration.get(Constants.SWIFT_AUTH_METHOD_KEY);
    if (authMethod != null) {
      config.setUsername(Configuration.get(Constants.SWIFT_USER_KEY));
      config.setTenantName(Configuration.get(Constants.SWIFT_TENANT_KEY));
      switch (authMethod) {
        case Constants.SWIFT_AUTH_KEYSTONE:
          config.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
          break;
        case Constants.SWIFT_AUTH_SWIFTAUTH:
          // swiftauth authenticates directly against swift
          // note: this method is supported in swift object storage api v1
          config.setAuthenticationMethod(AuthenticationMethod.BASIC);
          break;
        default:
          config.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
          // tempauth requires authentication header to be of the form tenant:user.
          // JOSS however generates header of the form user:tenant.
          // To resolve this, we switch user with tenant
          config.setTenantName(Configuration.get(Constants.SWIFT_USER_KEY));
          config.setUsername(Configuration.get(Constants.SWIFT_TENANT_KEY));
      }
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);
    mContainerName = containerName;
    mAccount = new AccountFactory(config).createAccount();
    mAccess = mAccount.authenticate();
    Container container = mAccount.getContainer(containerName);
    if (!container.exists()) {
      container.create();
    }
    mContainerPrefix = Constants.HEADER_SWIFT + mContainerName + PATH_SEPARATOR;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
  }

  @Override
  public void connectFromMaster(String hostname) {
    LOG.debug("connect from master");
  }

  @Override
  public void connectFromWorker(String hostname) {
    LOG.debug("connect from worker");
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, new CreateOptions());
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    LOG.debug("Create method: {}", path);

    // create will attempt to create the parent directory if it does not already exist
    if (!mkdirs(getParentKey(path), true)) {
      // fail if the parent directory does not exist and creation was unsuccessful
      LOG.error("Parent directory creation unsuccessful for {}", path);
      return null;
    }

    final String strippedPath = CommonUtils.stripPrefixIfPresent(path,
        Constants.HEADER_SWIFT);
    // TODO(adit): remove special handling of */_SUCCESS objects
    if (strippedPath.endsWith("_SUCCESS")) {
      // when path/_SUCCESS is created, there is need to create path as
      // an empty object. This is required by Spark in case Spark
      // accesses path directly, bypassing Alluxio
      String plainName = CommonUtils.stripSuffixIfPresent(strippedPath, "_SUCCESS");
      SwiftDirectClient.put(mAccess, plainName).close();
    }
    return SwiftDirectClient.put(mAccess, strippedPath);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: {}, recursive {}", path, recursive);
    String strippedPath = stripPrefixIfPresent(path);
    Container container = mAccount.getContainer(mContainerName);
    if (recursive) {
      strippedPath = addFolderSuffixIfNotPresent(strippedPath);
      PaginationMap paginationMap = container.getPaginationMap(strippedPath, DIR_PAGE_SIZE);
      for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
        for (StoredObject object : container.list(paginationMap, page)) {
          if (object.exists()) {
            object.delete();
          }
        }
      }
    }
    StoredObject object = container.getObject(strippedPath);
    if (object.exists()) {
      object.delete();
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // TODO(adit): remove special treatment of the _temporary suffix
    // To get better performance Swift driver does not create a _temporary folder.
    // This optimization should be hidden from Spark, therefore exists _temporary will return true.
    if (path.endsWith("_temporary")) {
      return true;
    }
    return isRoot(path) || isFile(path) || isDirectory(path);
  }

  /**
   * Gets the block size in bytes. There is no concept of a block in Swift and the maximum size of
   * one file is 4 GB. This method defaults to the default user block size in Alluxio.
   *
   * @param path the path to the object
   * @return the default Alluxio user block size
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Configuration.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public Object getConf() {
    LOG.debug("getConf is not supported when using SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using "
        + "SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.debug("getFileLocations is not supported when using "
        + "SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    return getObject(path).getContentLength();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    return getObject(path).getLastModifiedAsDate().getTime();
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    String pathAsFile = stripFolderSuffixIfPresent(path);
    return getObject(pathAsFile).exists();
  }

  @Override
  public String[] list(String path) throws IOException {
    return listInternal(path);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions().setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    if (path == null) {
      LOG.error("Attempting to create directory with a null path");
      return false;
    }
    if (isDirectory(path)) {
      return true;
    }
    if (isFile(path)) {
      LOG.error("Cannot create directory {} because it is already a file.", path);
      return false;
    }

    if (!parentExists(path)) {
      if (!options.getCreateParent()) {
        LOG.error("Cannot create directory {} because parent does not exist", path);
        return false;
      }
      final String parentKey = getParentKey(path);
      // Recursively make the parent folders
      if (!mkdirs(parentKey, true)) {
        LOG.error("Unable to create parent directory {}", path);
        return false;
      }
    }
    return mkdirsInternal(path);
  }

  /**
   * Creates a directory flagged file with the folder suffix.
   *
   * @param path the path to create a folder
   * @return true if the operation was successful, false otherwise
   */
  private boolean mkdirsInternal(String path) {
    try {
      String keyAsFolder = addFolderSuffixIfNotPresent(
          CommonUtils.stripPrefixIfPresent(path, Constants.HEADER_SWIFT));
      SwiftDirectClient.put(mAccess, keyAsFolder).close();
      return true;
    } catch (IOException e) {
      LOG.error("Failed to create directory: {}", path, e);
      return false;
    }
  }

  /**
   * Treating Swift as a file system, checks if the parent directory exists.
   *
   * @param path the path to check
   * @return true if the parent exists or if the path is root, false otherwise
   */
  private boolean parentExists(String path) throws IOException {
    final String parentKey = getParentKey(path);
    return parentKey != null && isDirectory(parentKey);
  }

  /**
   * @param path the path to get the parent of
   * @return the parent path, or null if path is root
   */
  private String getParentKey(String path) {
    // Root does not have a parent.
    if (isRoot(path)) {
      return null;
    }
    int separatorIndex = path.lastIndexOf(PATH_SEPARATOR);
    if (separatorIndex < 0) {
      LOG.error("Path {} is malformed", path);
      return null;
    }
    return path.substring(0, separatorIndex);
  }

  /**
   * Checks if the key is the root.
   *
   * @param key the key to check
   * @return true if the key is the root, false otherwise
   */
  private boolean isRoot(String key) {
    return PathUtils.normalizePath(key, PATH_SEPARATOR).equals(mContainerPrefix);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return getObject(path).downloadObjectAsInputStream();
  }

  /**
   * A trailing {@link SwiftUnderFileSystem#PATH_SEPARATOR} is added if not present.
   *
   * @param path URI to the object
   * @return folder path
   */
  private String addFolderSuffixIfNotPresent(String path) {
    return PathUtils.normalizePath(path, PATH_SEPARATOR);
  }

  /**
   * @inheritDoc
   *
   * @param source the source file or folder name
   * @param destination the destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean rename(String source, String destination) throws IOException {
    if (!exists(source)) {
      LOG.error("Unable to rename {} to {} because source does not exist.",
          source, destination);
      return false;
    }
    if (exists(destination)) {
      LOG.error("Unable to rename {} to {} because destination already exists.",
          source, destination);
      return false;
    }

    String strippedSourcePath = stripPrefixIfPresent(source);
    String strippedDestinationPath = stripPrefixIfPresent(destination);

    // Source exists and destination does not exist
    if (isDirectory(source)) {
      strippedSourcePath = addFolderSuffixIfNotPresent(strippedSourcePath);
      strippedDestinationPath = addFolderSuffixIfNotPresent(strippedDestinationPath);

      // Rename the source folder first
      if (!copy(strippedSourcePath, strippedDestinationPath)) {
        return false;
      }
      // Rename each child in the source folder to destination/child
      String [] children = list(source);
      for (String child: children) {
        // Recursive call
        if (!rename(PathUtils.concatPath(source, child),
            PathUtils.concatPath(destination, child))) {
          return false;
        }
      }
      // Delete source and everything under source
      return delete(source, true);
    }
    // Source is a file and destination does not exist
    return copy(strippedSourcePath, strippedDestinationPath) && delete(source, false);
  }

  @Override
  public void setConf(Object conf) {}

  // Not supported
  @Override
  public void setOwner(String path, String user, String group) {}

  // Not supported
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // Not supported
  @Override
  public String getOwner(String path) throws IOException {
    return null;
  }

  // Not supported
  @Override
  public String getGroup(String path) throws IOException {
    return null;
  }

  // Not supported
  @Override
  public short getMode(String path) throws IOException {
    return Constants.DEFAULT_FILE_SYSTEM_MODE;
  }

  /**
   * Copies an object to another name.
   *
   * @param source the source key to copy
   * @param destination the destination key to copy to
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String source, String destination) {
    LOG.debug("copy from {} to {}", source, destination);
    String strippedSourcePath = stripPrefixIfPresent(source);
    String strippedDestinationPath = stripPrefixIfPresent(destination);
    // Retry copy for a few times, in case some Swift internal errors happened during copy.
    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        Container container = mAccount.getContainer(mContainerName);
        container.getObject(strippedSourcePath)
            .copyObject(container, container.getObject(strippedDestinationPath));
        return true;
      } catch (Exception e) {
        LOG.error("Failed to copy file {} to {}", source, destination, e.getMessage());
        if (i != NUM_RETRIES - 1) {
          LOG.error("Retrying copying file {} to {}", source, destination);
        }
      }
    }
    LOG.error("Failed to copy file {} to {}, after {} retries", source, destination, NUM_RETRIES);
    return false;
  }

  /**
   * Checks if the path corresponds to a Swift directory.
   *
   * @param path the path to check
   * @return boolean indicating if the path is a directory
   * @throws IOException if an error occurs listing the directory
   */
  private boolean isDirectory(String path) throws IOException {
    // Root is always a folder
    if (isRoot(path)) {
      return true;
    }

    final String pathAsFolder = addFolderSuffixIfNotPresent(path);
    return getObject(pathAsFolder).exists();
  }

  /**
   * Lists the files in the given path, the returned paths will be their logical names and
   * not contain the folder suffix.
   *
   * @param path the key to list
   * @return an array of the file or folder names in this directory,
   * or null if the path does not exist
   * @throws IOException if path is not accessible, e.g. network issues
   */
  private String[] listInternal(final String path) throws IOException {
    String prefix = addFolderSuffixIfNotPresent(stripPrefixIfPresent(path));
    prefix = prefix.equals(PATH_SEPARATOR) ? "" : prefix;

    Directory directory = new Directory(prefix, PATH_SEPARATOR_CHAR);
    Container container = mAccount.getContainer(mContainerName);
    Collection<DirectoryOrObject> objects = container.listDirectory(directory);
    Set<String> children = new HashSet<>();
    boolean foundSelf = false;
    final String self = stripFolderSuffixIfPresent(prefix);
    for (DirectoryOrObject object : objects) {
      String child = stripFolderSuffixIfPresent(object.getName());
      String noPrefix = CommonUtils.stripPrefixIfPresent(child, prefix);
      if (!noPrefix.equals(self)) {
        children.add(noPrefix);
      } else {
        foundSelf = true;
      }
    }

    if (!foundSelf && (children.size() == 0)) {
      // Path does not exist
      return null;
    }

    return children.toArray(new String[children.size()]);
  }

  /**
   * Strips the folder suffix if it exists. This is a string manipulation utility and does not
   * guarantee the existence of the folder. This method will leave keys without a suffix unaltered.
   *
   * @param key the key to strip the suffix from
   * @return the key with the suffix removed, or the key unaltered if the suffix is not present
   */
  private String stripFolderSuffixIfPresent(final String key) {
    return CommonUtils.stripSuffixIfPresent(key, PATH_SEPARATOR);
  }

  /**
   * Strips the Swift container prefix from the key if it is present. For example, for input key
   * swift://my-container-name/my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param path the key to strip
   * @return the key without the Swift container prefix
   */
  private String stripPrefixIfPresent(final String path) {
    return CommonUtils.stripPrefixIfPresent(path, mContainerPrefix);
  }

  /**
   * Retrieves a handle to an object identified by the given path.
   *
   * @param path the path to retrieve an object handle for
   * @return the object handle
   */
  private StoredObject getObject(final String path) {
    Container container = mAccount.getContainer(mContainerName);
    return container.getObject(stripPrefixIfPresent(path));
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }
}
