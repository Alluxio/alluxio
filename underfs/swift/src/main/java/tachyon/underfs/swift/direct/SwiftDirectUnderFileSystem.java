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

package tachyon.underfs.swift.direct;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.swift.direct.http.SwiftDirectClient;

/**
 * Under file system implementation for OpenStack Swift based on
 * the JOSS library.
 */
public class SwiftDirectUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory */
  private static final String FOLDER_SUFFIX = "_$folder$";
  /** Value used to indicate nested structure in Swift */
  private static final String PATH_SEPARATOR = "/";

  /** Swift account */
  private final Account mAccount;
  /** Container name of user's configured Tachyon container */
  private final String mContainerName;
  /** Prefix of the container, for example swift://my-container-name/ */
  private final String mContainerPrefix;
  /** JOSS access object */
  private final Access mAccess;

  public SwiftDirectUnderFileSystem(String containerName,
      TachyonConf tachyonConf) throws Exception {
    super(tachyonConf);
    LOG.debug("Constructor init: {}", containerName);
    AccountConfig config = new AccountConfig();
    config.setUsername(tachyonConf.get(Constants.SWIFT_USER_KEY));
    config.setTenantName(tachyonConf.get(Constants.SWIFT_TENANT_KEY));
    config.setPassword(tachyonConf.get(Constants.SWIFT_API_KEY));
    config.setAuthUrl(tachyonConf.get(Constants.SWIFT_AUTH_URL_KEY));
    String authMethod = tachyonConf.get(Constants.SWIFT_AUTH_METHOD_KEY);
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
    LOG.debug("Check if container: {} exists ", containerName);
    Container containerObj = mAccount.getContainer(containerName);
    if (!containerObj.exists()) {
      containerObj.create();
    }
    mContainerPrefix = Constants.HEADER_SWIFT + mContainerName
        + PATH_SEPARATOR;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) {
    LOG.debug("connect from master");
  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) {
    LOG.debug("connect from worker");
  }

  @Override
  public OutputStream create(String path) throws IOException {
    LOG.debug("Create method: " + path);
    String newPath = path.substring(Constants.HEADER_SWIFT.length());
    SwiftDirectOutputStream out = SwiftDirectClient.PUT(mAccess, newPath);
    return out;
  }

  @Override
  public OutputStream create(String path,
      int blockSizeByte) throws IOException {
    LOG.warn("Create with block size is not supported"
        + "with SwiftDirectUnderFileSystem. Block size will be " + "ignored.");
    return create(path);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    LOG.warn("Create with block size and replication is not"
        + "supported with SwiftDirectUnderFileSystem."
        + " Block size and replication will be ignored.");
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: " + path + ",recursive: " + recursive);
    String strippedPath = stripPrefixIfPresent(path);
    LOG.debug("Going to retrieve container: " + mContainerName);
    Container c = mAccount.getContainer(mContainerName);
    LOG.debug("Going to retrieve an object: " + strippedPath);
    StoredObject so = c.getObject(strippedPath);
    LOG.debug("Checking if object exists in Swift. If so, delete it");
    if (so.exists()) {
      LOG.debug("Object exists: " + so.getBareName());
      so.delete();
      return true;
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    LOG.debug("Exists: " + path);
    String newPath = stripPrefixIfPresent(path);
    return getObjectDetails(newPath) ;
  }

  /**
   * Gets the StorageObject representing the metadata of a key. If the key does not exist as a
   * file or folder, null is returned
   * @param key the key to get the object details of
   * @return StorageObject of the key, or null if the key does not exist as a file or folder
   */
  private boolean getObjectDetails(String path) {
    LOG.debug("Get object details: " + path);
    boolean res =  mAccount.getContainer(mContainerName).getObject(path).exists();
    LOG.debug("Result: " + res);
    return res;
  }

  /**
   * There is no concept of a block in Swift, however the maximum allowed size of
   * one object is currently 4 GB.
   *
   * @param path to the file name
   * @return 4 GB in bytes
   * @throws IOException
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
    LOG.debug("Get object size: {}", path);
    StoredObject so = mAccount.getContainer(mContainerName).getObject(stripPrefixIfPresent(path));
    return so.getContentLength();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    LOG.debug("Get last modification time: {}", path);
    StoredObject so = mAccount.getContainer(mContainerName).getObject(stripPrefixIfPresent(path));
    return so.getLastModifiedAsDate().getTime();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    LOG.debug("is file: {}", path);
    return exists(path);
  }

  @Override
  public String[] list(String path) throws IOException {
    LOG.debug("listing: {}", path);
    path = path.endsWith(PATH_SEPARATOR) ? path : path + PATH_SEPARATOR;
    LOG.debug("listing: {}", path);
    return listInternal(path, false);
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    LOG.debug("mkdirs (skipped): {}, create parent: {}", path, createParent);
    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    LOG.debug("open method: {}", path);
    path = stripPrefixIfPresent(path);
    LOG.debug("Going to get container");
    Container container = mAccount.getContainer(mContainerName);
    LOG.debug("Going to get object {} for container {}", path, mContainerName);
    StoredObject so = container.getObject(path);
    LOG.debug("Got an object. Going to download");
    InputStream  is = so.downloadObjectAsInputStream();
    LOG.debug("Got input stream for: {}", path);
    return is;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    LOG.debug("rename: {}, dest: {}", src, dst);
    if (!exists(src)) {
      LOG.error("Unable to rename {} to {}. Source does not exists", src, dst);
      return false;
    }
    copy(src, dst);
    return delete(src, true);
  }

  @Override
  public void setConf(Object conf) {
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
  }

  /**
   * Copies an object to another key.
   *
   * @param src the source key to copy
   * @param dst the destination key to copy to
   * @return true if the operation was successful, false otherwise
   */
  private boolean copy(String src, String dst) {
    LOG.debug("copy from {} to {}", src, dst);
    try {
      Container container = mAccount.getContainer(mContainerName);
      container.getObject(stripPrefixIfPresent(src)).copyObject(container,
          container.getObject(stripPrefixIfPresent(dst)));
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  /**
   * Lists the files in the given path, the paths will be their logical names
   * and not contain the folder suffix
   *
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
      Directory directory = new Directory(path, '/');
      LOG.debug("Path to list is: {}", path);
      Container c = mAccount.getContainer(mContainerName);
      Collection<DirectoryOrObject> res = c.listDirectory(directory);
      Set<String> children = new HashSet<String>();
      Iterator<DirectoryOrObject> iter = res.iterator();
      LOG.debug("Going to list with directory structure");
      while (iter.hasNext()) {
        LOG.trace("Inside loop of results");
        DirectoryOrObject dirobj = (DirectoryOrObject) iter.next();
        LOG.trace(dirobj.getName() + ", " + dirobj.getBareName());
        LOG.trace("Going to strip suffix is present");
        String child = stripFolderSuffixIfPresent(dirobj.getName());
        LOG.trace("Object: {}, child: {}", dirobj.getName(), child);
        String noPrefix = stripPrefixIfPresent(child, path);
        LOG.trace("Without prefix: {}", noPrefix);
        children.add(noPrefix);
      }
      LOG.debug("Children's size is: {}", children.size());
      return children.toArray(new String[children.size()]);
    } catch (Exception se) {
      LOG.error("Failed to list path {}", path);
      return null;
    }
  }

  /**
   * Strip the folder suffix if it exists. This is a string manipulation utility
   * and does not guarantee the existence of the folder. This method will leave
   * keys without a suffix unaltered.
   *
   * @param key the key to strip the suffix from
   * @return the key with the suffix removed, or the key unaltered if the suffix
   *         is not present
   */
  private String stripFolderSuffixIfPresent(String key) {
    if (key.endsWith(FOLDER_SUFFIX)) {
      return key.substring(0, key.length() - FOLDER_SUFFIX.length());
    }
    return key;
  }

  /**
   * Strips the Swift container prefix from the key if it is present. For example, for
   * input key swift://my-container-name/my-path/file, the output would be
   * my-path/file. This method will leave keys without a prefix unaltered, ie.
   * my-path/file returns my-path/file.
   *
   * @param key
   *          the key to strip
   * @return the key without the Swift container prefix
   */
  private String stripPrefixIfPresent(String path) {
    return stripPrefixIfPresent(path, mContainerPrefix);
  }

  /**
   * Strips the Swift container prefix from the key if it is present. For example, for
   * input key swift://my-container-name/my-path/file, the output would be
   * my-path/file. This method will leave keys without a prefix unaltered, ie.
   * my-path/file returns my-path/file.
   *
   * @param key
   *          the key to strip
   * @return the key without the Swift container prefix
   */
  private String stripPrefixIfPresent(String path, String prefix) {
    LOG.debug("{}, prefix: {}" , path.toString(), prefix);
    if (path.startsWith(prefix)) {
      String res =  path.substring(prefix.length());
      LOG.debug("Resolved as: {}", res);
      return res;
    }
    LOG.warn("Attempted to strip key {} with invalid prefix {}", prefix, path);
    return path;
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.SWIFT;
  }
}
