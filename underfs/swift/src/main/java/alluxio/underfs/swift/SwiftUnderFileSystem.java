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

package alluxio.underfs.swift;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OpenStack Swift {@link UnderFileSystem} implementation based on the JOSS library.
 */
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
   * @param containerName the name of the container
   * @param configuration the configuration for Alluxio
   */
  public SwiftUnderFileSystem(String containerName,
      Configuration configuration) {
    super(configuration);
    LOG.debug("Constructor init: {}", containerName);
    AccountConfig config = new AccountConfig();
    config.setUsername(configuration.get(Constants.SWIFT_USER_KEY));
    config.setTenantName(configuration.get(Constants.SWIFT_TENANT_KEY));
    config.setPassword(configuration.get(Constants.SWIFT_API_KEY));
    config.setAuthUrl(configuration.get(Constants.SWIFT_AUTH_URL_KEY));
    String authMethod = configuration.get(Constants.SWIFT_AUTH_METHOD_KEY);
    if (authMethod != null && authMethod.equals("keystone")) {
      config.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
    } else {
      config.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);
    mContainerName = containerName;
    mAccount = new AccountFactory(config).createAccount();
    mAccess = mAccount.authenticate();
    Container containerObj = mAccount.getContainer(containerName);
    if (!containerObj.exists()) {
      containerObj.create();
    }
    mContainerPrefix = Constants.HEADER_SWIFT + mContainerName + PATH_SEPARATOR;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
  }

  @Override
  public void connectFromMaster(Configuration conf, String hostname) {
    LOG.debug("connect from master");
  }

  @Override
  public void connectFromWorker(Configuration conf, String hostname) {
    LOG.debug("connect from worker");
  }

  @Override
  public OutputStream create(String path) throws IOException {
    LOG.debug("Create method: {}", path);
    String newPath = path.substring(Constants.HEADER_SWIFT.length());
    SwiftOutputStream out = SwiftDirectClient.put(mAccess, newPath);
    return out;
  }

  @Override
  public OutputStream create(String path,
      int blockSizeByte) throws IOException {
    LOG.warn("Create with block size is not supported"
        + "with SwiftDirectUnderFileSystem. Block size will be ignored.");
    return create(path);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    LOG.warn("Create with block size and replication is not"
        + "supported with SwiftDirectUnderFileSystem."
        + " Block size and replication will be ignored.");
    return create(path);
  }

  /**
   * @inheritDoc
   *
   * @param path the file or folder name
   * @param recursive whether we delete folder and its children
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: {}, recursive {}", path, recursive);
    String strippedPath = stripPrefixIfPresent(path);
    Container c = mAccount.getContainer(mContainerName);
    if (recursive) {
      strippedPath = makeQualifiedPath(strippedPath);
      PaginationMap paginationMap = c.getPaginationMap(strippedPath, 100);
      for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
        for (StoredObject obj : c.list(paginationMap, page)) {
          if (obj.exists()) {
            obj.delete();
          }
        }
      }
    }
    StoredObject so = c.getObject(strippedPath);
    if (so.exists()) {
      so.delete();
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    String newPath = stripPrefixIfPresent(path);
    return isObjectExists(newPath);
  }

  /**
   * Checks if the object exists.
   *
   * @param path the key to get the object details of
   * @return boolean indicating if the object exists
   */
  private boolean isObjectExists(String path) {
    LOG.debug("Checking if {} exists", path);
    boolean res =  mAccount.getContainer(mContainerName).getObject(path).exists();
    return res;
  }

  /**
   * Gets the block size in bytes. There is no concept of a block in Swift, however the maximum
   * allowed size of one file is currently 4 GB.
   *
   * @param path to the object
   * @return 4 GB in bytes
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Constants.GB * 4;
  }

  @Override
  public Object getConf() {
    LOG.warn("getConf is not supported when using SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using "
        + "SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.warn("getFileLocations is not supported when using "
        + "SwiftDirectUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    StoredObject so = mAccount.getContainer(mContainerName).getObject(stripPrefixIfPresent(path));
    return so.getContentLength();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    StoredObject so = mAccount.getContainer(mContainerName).getObject(stripPrefixIfPresent(path));
    return so.getLastModifiedAsDate().getTime();
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return exists(path);
  }

  @Override
  public String[] list(String path) throws IOException {
    path = PathUtils.normalizePath(path, PATH_SEPARATOR);
    return listInternal(path, false);
  }

  /**
   * @inheritDoc
   *
   * @param path the folder to create
   * @param createParent if true, the method creates any necessary but nonexistent parent
   *        directories; otherwise, the method does not create nonexistent parent directories
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    path = stripPrefixIfPresent(path);
    Container container = mAccount.getContainer(mContainerName);
    StoredObject so = container.getObject(path);
    InputStream  is = so.downloadObjectAsInputStream();
    return is;
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
    Container c = mAccount.getContainer(mContainerName);
    strippedSrcPath = makeQualifiedPath(strippedSrcPath);
    strippedDstPath = makeQualifiedPath(strippedDstPath);
    PaginationMap paginationMap = c.getPaginationMap(strippedSrcPath, 100);
    for (int page = 0; page < paginationMap.getNumberOfPages(); page++) {
      for (StoredObject obj : c.list(paginationMap, page)) {
        if (obj.exists() && copy(obj.getName(),
            obj.getName().replace(strippedSrcPath, strippedDstPath))) {
          delete(obj.getName(), false);
        }
      }
    }
    return true;
  }

  @Override
  public void setConf(Object conf) {
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
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
    try {
      Container container = mAccount.getContainer(mContainerName);
      container.getObject(src).copyObject(container,
          container.getObject(dst));
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  /**
   * Lists the files in the given path, the paths will be their logical names and not contain the
   * folder suffix.
   *
   * @param path the key to list
   * @param recursive if true will list children directories as well
   * @return an array of the file and folder names in this directory
   * @throws IOException if path is not accessible, e.g. network issues
   */
  private String[] listInternal(String path, boolean recursive) throws IOException {
    try {
      path = stripPrefixIfPresent(path);
      path = PathUtils.normalizePath(path, PATH_SEPARATOR);
      path = path.equals(PATH_SEPARATOR) ? "" : path;
      Directory directory = new Directory(path, '/');
      Container c = mAccount.getContainer(mContainerName);
      Collection<DirectoryOrObject> res = c.listDirectory(directory);
      Set<String> children = new HashSet<String>();
      Iterator<DirectoryOrObject> iter = res.iterator();
      while (iter.hasNext()) {
        DirectoryOrObject dirobj = (DirectoryOrObject) iter.next();
        String child = stripFolderSuffixIfPresent(dirobj.getName());
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
      String res =  path.substring(prefix.length());
      return res;
    }
    return path;
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }
}
