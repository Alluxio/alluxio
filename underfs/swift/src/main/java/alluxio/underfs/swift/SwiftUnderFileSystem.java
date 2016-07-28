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

import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.exception.NotFoundException;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.PaginationMap;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OpenStack Swift {@link UnderFileSystem} implementation based on the JOSS library.
 * The mkdir operation creates a zero-byte object.
 * A suffix {@link SwiftUnderFileSystem#PATH_SEPARATOR} in the object name denotes a folder.
 * JOSS directory listing API requires that the suffix be a single character.
 */
// TODO(adit): Abstract out functionality common with other object under storage systems.
@ThreadSafe
public class SwiftUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Value used to indicate nested structure in Swift. */
  private static final char PATH_SEPARATOR_CHAR = '/';

  /** Value used to indicate nested structure in Swift. */
  private static final String PATH_SEPARATOR = String.valueOf(PATH_SEPARATOR_CHAR);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = PATH_SEPARATOR;

  /** Maximum number of directory entries to fetch at once. */
  private static final int DIR_PAGE_SIZE = 1000;

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

  /** Determine whether to run JOSS in simulation mode. */
  private boolean mSimulationMode;

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

    // Whether to run against a simulated Swift backend
    mSimulationMode = false;
    if (Configuration.containsKey(Constants.SWIFT_SIMULATION)) {
      mSimulationMode = Configuration.getBoolean(Constants.SWIFT_SIMULATION);
    }

    if (mSimulationMode) {
      // In simulation mode we do not need access credentials
      config.setMock(true);
      config.setMockAllowEveryone(true);
    } else {
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
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);
    mContainerName = containerName;
    mAccount = new AccountFactory(config).createAccount();
    // Do not allow container cache to avoid stale directory listings
    mAccount.setAllowContainerCaching(false);
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
    if (!mkdirs(getParentPath(path), true)) {
      // fail if the parent directory does not exist and creation was unsuccessful
      LOG.error("Parent directory creation unsuccessful for {}", path);
      return null;
    }

    // TODO(adit): remove special handling of */_SUCCESS objects
    if (path.endsWith("_SUCCESS")) {
      // when path/_SUCCESS is created, there is need to create path as
      // an empty object. This is required by Spark in case Spark
      // accesses path directly, bypassing Alluxio
      createOutputStream(CommonUtils.stripSuffixIfPresent(path, "_SUCCESS")).close();
    }

    return createOutputStream(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: {}, recursive {}", path, recursive);
    final String strippedPath = stripContainerPrefixIfPresent(path);
    Container container = mAccount.getContainer(mContainerName);
    if (recursive) {
      // For a file, recursive delete will not find any children
      PaginationMap paginationMap = container.getPaginationMap(
          addFolderSuffixIfNotPresent(strippedPath), DIR_PAGE_SIZE);
      for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
        for (StoredObject childObject : container.list(paginationMap, page)) {
          deleteObject(childObject);
        }
      }
    } else {
      String[] children = list(path);
      if (children != null && children.length != 0) {
        LOG.error("Attempting to non-recursively delete a non-empty directory.");
        return false;
      }
    }

    // Path is a file or folder with no children
    if (!deleteObject(container.getObject(strippedPath))) {
      // Path may be a folder
      final String strippedFolderPath = addFolderSuffixIfNotPresent(strippedPath);
      if (strippedFolderPath.equals(strippedPath)) {
        return false;
      }
      return deleteObject(container.getObject(strippedFolderPath));
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // TODO(adit): remove special treatment of the _temporary suffix
    // To get better performance Swift driver does not create a _temporary folder.
    // This optimization should be hidden from Spark, therefore exists _temporary will return true.
    if (isRoot(path) || path.endsWith("_temporary")) {
      return true;
    }

    // Query file or folder using single listing query
    Collection<DirectoryOrObject> objects =
        listInternal(stripFolderSuffixIfPresent(stripContainerPrefixIfPresent(path)), false);
    return objects != null && objects.size() != 0;
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
  public String[] listRecursive(String path) throws IOException {
    return listHelper(path, true);
  }

  @Override
  public String[] list(String path) throws IOException {
    return listHelper(path, false);
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
      final String parentPath = getParentPath(path);
      // TODO(adit): See how we can do this with better performance
      // Recursively make the parent folders
      if (!mkdirs(parentPath, true)) {
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
      // We do not check if a file with same name exists, i.e. a file with name
      // 'swift://swift-container/path' and a folder with name 'swift://swift-container/path/'
      // may both exist simultaneously
      createOutputStream(addFolderSuffixIfNotPresent(path)).close();
      return true;
    } catch (IOException e) {
      LOG.error("Failed to create directory: {}", path, e);
      return false;
    }
  }

  /**
   * Checks if the parent directory exists, treating Swift as a file system.
   *
   * @param path the path to check
   * @return true if the parent exists or if the path is root, false otherwise
   */
  private boolean parentExists(String path) throws IOException {
    final String parentPath = getParentPath(path);
    return parentPath != null && isDirectory(parentPath);
  }

  /**
   * @param path the path to get the parent of
   * @return the parent path, or null if path is root
   */
  private String getParentPath(String path) {
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
   * Checks if the path is the root.
   *
   * @param path the path to check
   * @return true if the path is the root, false otherwise
   */
  private boolean isRoot(final String path) {
    return addFolderSuffixIfNotPresent(path).equals(mContainerPrefix);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return getObject(path).downloadObjectAsInputStream();
  }

  /**
   * A trailing {@link SwiftUnderFileSystem#FOLDER_SUFFIX} is added if not present.
   *
   * @param path URI to the object
   * @return folder path
   */
  private String addFolderSuffixIfNotPresent(final String path) {
    return PathUtils.normalizePath(path, FOLDER_SUFFIX);
  }

  /**
   * @inheritDoc
   * Rename will overwrite destination if it already exists
   *
   * @param source the source file or folder name
   * @param destination the destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean rename(String source, String destination) throws IOException {
    String strippedSourcePath = stripContainerPrefixIfPresent(source);
    String strippedDestinationPath = stripContainerPrefixIfPresent(destination);

    if (isDirectory(destination)) {
      // If destination is a directory target is a file or folder within that directory
      strippedDestinationPath = PathUtils.concatPath(strippedDestinationPath,
          FilenameUtils.getName(stripFolderSuffixIfPresent(strippedSourcePath)));
    }

    if (isDirectory(source)) {
      // Source is a directory
      strippedSourcePath = addFolderSuffixIfNotPresent(strippedSourcePath);
      strippedDestinationPath = addFolderSuffixIfNotPresent(strippedDestinationPath);

      // Rename the source folder first
      if (!copy(strippedSourcePath, strippedDestinationPath)) {
        return false;
      }
      // Rename each child in the source folder to destination/child
      String [] children = list(source);
      for (String child: children) {
        // TODO(adit): See how we can do this with better performance
        // Recursive call
        if (!rename(PathUtils.concatPath(source, child),
            PathUtils.concatPath(mContainerPrefix,
                PathUtils.concatPath(strippedDestinationPath, child)))) {
          return false;
        }
      }
      // Delete source and everything under source
      return delete(source, true);
    }

    // Source is a file and destination is also a file
    return copy(strippedSourcePath, strippedDestinationPath) && delete(source, false);
  }

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
   * Copies an object to another name. Destination will be overwritten if it already exists.
   *
   * @param source the source path to copy
   * @param destination the destination path to copy to
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String source, String destination) {
    LOG.debug("copy from {} to {}", source, destination);
    final String strippedSourcePath = stripContainerPrefixIfPresent(source);
    final String strippedDestinationPath = stripContainerPrefixIfPresent(destination);
    // Retry copy for a few times, in case some Swift internal errors happened during copy.
    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        Container container = mAccount.getContainer(mContainerName);
        container.getObject(strippedSourcePath)
            .copyObject(container, container.getObject(strippedDestinationPath));
        return true;
      } catch (NotFoundException e) {
        LOG.error("Source path {} does not exist", source);
        return false;
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
   * Lists the files or folders in the given path, not including the path itself.
   *
   * @param path the folder path whose children are listed
   * @param recursive whether to do a recursive listing
   * @return a collection of the files or folders in the given path, or null if path is a file or
   * does not exist
   * @throws IOException if path is not accessible, e.g. network issues
   */
  private String[] listHelper(String path, boolean recursive) throws IOException {
    String prefix = PathUtils.normalizePath(stripContainerPrefixIfPresent(path), PATH_SEPARATOR);
    prefix = prefix.equals(PATH_SEPARATOR) ? "" : prefix;

    Collection<DirectoryOrObject> objects = listInternal(prefix, recursive);
    Set<String> children = new HashSet<>();
    final String self = stripFolderSuffixIfPresent(prefix);
    boolean foundSelf = false;
    for (DirectoryOrObject object : objects) {
      String child = stripFolderSuffixIfPresent(object.getName());
      String noPrefix = CommonUtils.stripPrefixIfPresent(child, prefix);
      if (!noPrefix.equals(self)) {
        children.add(noPrefix);
      } else {
        foundSelf = true;
      }
    }

    if (!foundSelf) {
      if (mSimulationMode) {
        if (children.size() != 0 || isDirectory(path)) {
          // In simulation mode, the JOSS listDirectory call does not return the prefix itself,
          // so we need the extra isDirectory call
          foundSelf = true;
        }
      }

      if (!foundSelf) {
        // Path does not exist
        return null;
      }
    }

    return children.toArray(new String[children.size()]);
  }

  /**
   * Lists the files or folders which match the given prefix using pagination.
   *
   * @param prefix the prefix to match
   * @param recursive whether to do a recursive listing
   * @return a collection of the files or folders matching the prefix, or null if not found
   * @throws IOException if path is not accessible, e.g. network issues
   */
  private Collection<DirectoryOrObject> listInternal(final String prefix, boolean recursive)
      throws IOException {
    // TODO(adit): UnderFileSystem interface should be changed to support pagination
    ArrayDeque<DirectoryOrObject> results = new ArrayDeque<>();
    Container container = mAccount.getContainer(mContainerName);
    PaginationMap paginationMap = container.getPaginationMap(prefix, DIR_PAGE_SIZE);
    for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
      if (!recursive) {
        // If not recursive, use delimiter to limit results fetched
        results.addAll(container.listDirectory(paginationMap.getPrefix(), PATH_SEPARATOR_CHAR,
            paginationMap.getMarker(page), paginationMap.getPageSize()));
      } else {
        results.addAll(container.list(paginationMap, page));
      }
    }
    return results;
  }

  /**
   * Strips the folder suffix if it exists. This is a string manipulation utility and does not
   * guarantee the existence of the folder. This method will leave paths without a suffix unaltered.
   *
   * @param path the path to strip the suffix from
   * @return the path with the suffix removed, or the path unaltered if the suffix is not present
   */
  private String stripFolderSuffixIfPresent(final String path) {
    return CommonUtils.stripSuffixIfPresent(path, FOLDER_SUFFIX);
  }

  /**
   * Strips the Swift container prefix from the path if it is present. For example, for input path
   * swift://my-container-name/my-path/file, the output would be my-path/file. This method will
   * leave paths without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param path the path to strip
   * @return the path without the Swift container prefix
   */
  private String stripContainerPrefixIfPresent(final String path) {
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
    return container.getObject(stripContainerPrefixIfPresent(path));
  }

  /**
   * Deletes an object if it exists.
   *
   * @param object object handle to delete
   * @return true if object deletion was successful
   */
  private boolean deleteObject(final StoredObject object) {
    try {
      object.delete();
      return true;
    } catch (NotFoundException e) {
      LOG.debug("Object {} not found", object.getPath());
    }
    return false;
  }

  /**
   * Creates a simulated or actual OutputStream for object uploads.
   * @throws IOException if failed to create path
   * @return new OutputStream
   */
  private OutputStream createOutputStream(String path) throws IOException {
    if (mSimulationMode) {
      return new SwiftMockOutputStream(mAccount, mContainerName,
          stripContainerPrefixIfPresent(path));
    }

    return SwiftDirectClient.put(mAccess,
        CommonUtils.stripPrefixIfPresent(path, Constants.HEADER_SWIFT));
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }
}
