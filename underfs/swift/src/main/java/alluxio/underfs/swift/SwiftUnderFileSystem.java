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
 * OpenStack Swift {@link UnderFileSystem} implementation based on the JOSS library. This
 * implementation does not support the concept of directories due to Swift being an object store.
 * All mkdir operations will no-op and return true and empty directories will not exist.
 * Directories with objects inside will be inferred through the prefix.
 */
//TODO(calvin): Reconsider the directory limitations of this class.
@ThreadSafe
public class SwiftUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Value used to indicate nested structure in Swift. */
  private static final String PATH_SEPARATOR = "/";

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
    String newPath = path.substring(Constants.HEADER_SWIFT.length());
    if (newPath.endsWith("_SUCCESS")) {
      // when path/_SUCCESS is created, there is need to create path as
      // an empty object. This is required by Spark in case Spark
      // accesses path directly, bypassing Alluxio
      String plainName = newPath.substring(0, newPath.indexOf("_SUCCESS"));
      LOG.debug("Plain name: {}", plainName);
      SwiftOutputStream out = SwiftDirectClient.put(mAccess, plainName);
      out.close();
    }
    return SwiftDirectClient.put(mAccess, newPath);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: {}, recursive {}", path, recursive);
    String strippedPath = stripPrefixIfPresent(path);
    Container container = mAccount.getContainer(mContainerName);
    if (recursive) {
      strippedPath = makeQualifiedPath(strippedPath);
      PaginationMap paginationMap = container.getPaginationMap(strippedPath, 100);
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
    // To get better performance Swift driver does not create a _temporary folder.
    // This optimization should be hidden from Spark, therefore exists _temporary will return true.
    if (path.endsWith("_temporary")) {
      return true;
    }
    return getObject(path).exists();
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
    return exists(path) && !isDirectory(path);
  }

  @Override
  public String[] list(String path) throws IOException {
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    return listInternal(path);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions().setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return getObject(path).downloadObjectAsInputStream();
  }

  /**
   * Each path is checked both for leading "/" and ending "/". Leading "/" is removed, and "/" is
   * added at the end if not present.
   *
   * @param path URI to the object
   * @return qualified path
   */
  private String makeQualifiedPath(String path) {
    if (!path.endsWith("/")) {
      path = path + "/";
    }
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }

  /**
   * @inheritDoc
   *
   * @param src the source file or folder name
   * @param dst the destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean rename(String src, String dst) throws IOException {
    String strippedSrcPath = stripPrefixIfPresent(src);
    String strippedDstPath = stripPrefixIfPresent(dst);
    if (exists(src) && copy(strippedSrcPath, strippedDstPath)) {
      return delete(src, true);
    }
    Container container = mAccount.getContainer(mContainerName);
    strippedSrcPath = makeQualifiedPath(strippedSrcPath);
    strippedDstPath = makeQualifiedPath(strippedDstPath);
    PaginationMap paginationMap = container.getPaginationMap(strippedSrcPath, 100);
    for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
      for (StoredObject object : container.list(paginationMap, page)) {
        if (object.exists() && copy(object.getName(),
            object.getName().replace(strippedSrcPath, strippedDstPath))) {
          delete(object.getName(), false);
        }
      }
    }
    return true;
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
   * @param src the source key to copy
   * @param dst the destination key to copy to
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String src, String dst) {
    LOG.debug("copy from {} to {}", src, dst);
    String strippedSrcPath = stripPrefixIfPresent(src);
    String strippedDstPath = stripPrefixIfPresent(dst);
    // Retry copy for a few times, in case some Swift internal errors happened during copy.
    int retries = 3;
    for (int i = 0; i < retries; i++) {
      try {

        Container container = mAccount.getContainer(mContainerName);
        container.getObject(strippedSrcPath)
            .copyObject(container, container.getObject(strippedDstPath));
        return true;
      } catch (Exception e) {
        LOG.error("Failed to copy file {} to {}", src, dst, e.getMessage());
        if (i != retries - 1) {
          LOG.error("Retrying copying file {} to {}", src, dst);
        }
      }
    }
    LOG.error("Failed to copy file {} to {}, after {} retries", src, dst, retries);
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
    String[] children = listInternal(path);
    return children != null && children.length > 0;
  }

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix.
   *
   * @param path the key to list
   * @return an array of the file and folder names in this directory
   * @throws IOException if path is not accessible, e.g. network issues
   */
  private String[] listInternal(String path) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      path = PathUtils.normalizePath(path, PATH_SEPARATOR);
      path = path.equals(PATH_SEPARATOR) ? "" : path;
      Directory directory = new Directory(path, '/');
      Container container = mAccount.getContainer(mContainerName);
      Collection<DirectoryOrObject> objects = container.listDirectory(directory);
      Set<String> children = new HashSet<>();
      for (DirectoryOrObject object : objects) {
        String child = stripFolderSuffixIfPresent(object.getName());
        String noPrefix = stripPrefixIfPresent(child, path);
        children.add(noPrefix);
      }
      return children.toArray(new String[children.size()]);
    } catch (Exception e) {
      LOG.error("Failed to list path {}", path, e);
      return null;
    }
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
   * Strips the Swift container prefix from the key if it is present. For example, for input key
   * swift://my-container-name/my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param path the key to strip
   * @return the key without the Swift container prefix
   */
  private String stripPrefixIfPresent(String path) {
    if (path.startsWith(PATH_SEPARATOR)) {
      return stripPrefixIfPresent(path, PATH_SEPARATOR);
    }
    return stripPrefixIfPresent(path, mContainerPrefix);
  }

  /**
   * Strips the Swift container prefix from the key if it is present. For example, for input key
   * swift://my-container-name/my-path/file, the output would be my-path/file. This method will
   * leave keys without a prefix unaltered, ie. my-path/file returns my-path/file.
   *
   * @param path the key to strip
   * @param prefix prefix to remove
   * @return the key without the Swift container prefix
   */
  private String stripPrefixIfPresent(String path, String prefix) {
    if (path.startsWith(prefix)) {
      return path.substring(prefix.length());
    }
    return path;
  }

  /**
   * Retrieves a handle to an object identified by the given path.
   *
   * @param path the path to retrieve an object handle for
   * @return the object handle
   */
  private StoredObject getObject(String path) {
    String strippedPath = stripPrefixIfPresent(path);
    Container container = mAccount.getContainer(mContainerName);
    return container.getObject(strippedPath);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }
}
