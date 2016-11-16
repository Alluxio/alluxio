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

package alluxio.underfs.local;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Local FS {@link UnderFileSystem} implementation.
 * <p>
 * This is primarily intended for local unit testing and single machine mode. In principle, it can
 * also be used on a system where a shared file system (e.g. NFS) is mounted at the same path on
 * every node of the system. However, it is generally preferable to use a proper distributed file
 * system for that scenario.
 * </p>
 */
@ThreadSafe
public class LocalUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link LocalUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public LocalUnderFileSystem(AlluxioURI uri) {
    super(uri);
  }

  @Override
  public String getUnderFSType() {
    return "local";
  }

  @Override
  public void close() throws IOException {}

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    path = stripPath(path);
    OutputStream stream = new FileOutputStream(path);
    try {
      setMode(path, options.getPermission().getMode().toShort());
    } catch (IOException e) {
      stream.close();
      throw e;
    }
    return stream;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    boolean success = true;
    if (recursive && file.isDirectory()) {
      String[] files = file.list();

      // File.list() will return null if an I/O error occurs.
      // e.g.: Reading an non-readable directory
      if (files != null) {
        for (String child : files) {
          success = success && delete(PathUtils.concatPath(path, child), true);
        }
      }
    }

    return success && file.delete();
  }

  @Override
  public boolean exists(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.exists();
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(path);
    }
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<>();
    ret.add(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC));
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    return getFileLocations(path);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.length();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.lastModified();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    switch (type) {
      case SPACE_TOTAL:
        return file.getTotalSpace();
      case SPACE_FREE:
        return file.getFreeSpace();
      case SPACE_USED:
        return file.getTotalSpace() - file.getFreeSpace();
      default:
        throw new IOException("Unknown getSpace parameter: " + type);
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.isFile();
  }

  @Override
  public String[] list(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    File[] files = file.listFiles();
    if (files != null) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (File f : files) {
        rtn[i++] = f.getName();
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions().setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    Permission perm = options.getPermission();
    if (!options.getCreateParent()) {
      if (file.mkdir()) {
        setMode(file.getPath(), perm.getMode().toShort());
        FileUtils.setLocalDirStickyBit(file.getPath());
        try {
          setOwner(file.getPath(), perm.getOwner(), perm.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update the ufs dir ownership, default values will be used. " + e);
        }
        return true;
      }
      return false;
    }
    // Create parent directories one by one and set their permissions to rwxrwxrwx.
    Stack<File> dirsToMake = new Stack<>();
    dirsToMake.push(file);
    File parent = file.getParentFile();
    while (!parent.exists()) {
      dirsToMake.push(parent);
      parent = parent.getParentFile();
    }
    while (!dirsToMake.empty()) {
      File dirToMake = dirsToMake.pop();
      if (dirToMake.mkdir()) {
        setMode(dirToMake.getAbsolutePath(), perm.getMode().toShort());
        FileUtils.setLocalDirStickyBit(file.getPath());
        // Set the owner to the Alluxio client user to achieve permission delegation.
        // Alluxio server-side user is required to be a superuser. If it fails to set owner,
        // proceeds with mkdirs and print out an warning message.
        try {
          setOwner(dirToMake.getAbsolutePath(), perm.getOwner(), perm.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update the ufs dir ownership, default values will be used. " + e);
        }
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    path = stripPath(path);
    return new FileInputStream(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    src = stripPath(src);
    dst = stripPath(dst);
    File file = new File(src);
    return file.renameTo(new File(dst));
  }

  @Override
  public void setConf(Object conf) {}

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    path = stripPath(path);
    try {
      if (user != null) {
        FileUtils.changeLocalFileUser(path, user);
      }
      if (group != null) {
        FileUtils.changeLocalFileGroup(path, group);
      }
    } catch (IOException e) {
      LOG.error("Fail to set owner for {} with user: {}, group: {}", path, user, group);
      LOG.warn("In order for Alluxio to set local files with the correct user and groups, "
          + "Alluxio should be the local file system superusers.");
      if (!Configuration.getBoolean(PropertyKey.UNDERFS_ALLOW_SET_OWNER_FAILURE)) {
        throw e;
      } else {
        LOG.warn("Proceeding... but this may cause permission inconsistency between Alluxio and "
            + "local under file system.");
      }
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    path = stripPath(path);
    String posixPerm = new Mode(mode).toString();
    FileUtils.changeLocalFilePermission(path, posixPerm);
  }

  @Override
  public String getOwner(String path) throws IOException {
    path = stripPath(path);
    return FileUtils.getLocalFileOwner(path);
  }

  @Override
  public String getGroup(String path) throws IOException {
    path = stripPath(path);
    return FileUtils.getLocalFileGroup(path);
  }

  @Override
  public short getMode(String path) throws IOException {
    path = stripPath(path);
    return FileUtils.getLocalFileMode(path);
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }
}
