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
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.security.authorization.Mode;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
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
public class LocalUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(LocalUnderFileSystem.class);

  /**
   * Constructs a new {@link LocalUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
  public LocalUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
  }

  @Override
  public String getUnderFSType() {
    return "local";
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    path = stripPath(path);
    if (options.getCreateParent()) {
      File parent = new File(path).getParentFile();
      if (parent != null && !parent.mkdirs() && !parent.isDirectory()) {
        throw new IOException(ExceptionMessage.PARENT_CREATION_FAILED.getMessage(path));
      }
    }
    OutputStream stream = new FileOutputStream(path);
    try {
      setMode(path, options.getMode().toShort());
    } catch (IOException e) {
      stream.close();
      throw e;
    }
    return stream;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    if (!file.isDirectory()) {
      return false;
    }
    boolean success = true;
    if (options.isRecursive()) {
      String[] files = file.list();

      // File.list() will return null if an I/O error occurs.
      // e.g.: Reading an non-readable directory
      if (files != null) {
        for (String child : files) {
          String childPath = PathUtils.concatPath(path, child);
          if (isDirectory(childPath)) {
            success = success && deleteDirectory(childPath,
                DeleteOptions.defaults().setRecursive(true));
          } else {
            success = success && deleteFile(PathUtils.concatPath(path, child));
          }
        }
      }
    }
    return success && file.delete();
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.isFile() && file.delete();
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
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    String tpath = stripPath(path);
    File file = new File(tpath);
    PosixFileAttributes attr =
        Files.readAttributes(Paths.get(file.getPath()), PosixFileAttributes.class);
    return new UfsDirectoryStatus(path, attr.owner().getName(), attr.group().getName(),
        FileUtils.translatePosixPermissionToMode(attr.permissions()));
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<>();
    ret.add(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC));
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    String tpath = stripPath(path);
    File file = new File(tpath);
    PosixFileAttributes attr =
        Files.readAttributes(Paths.get(file.getPath()), PosixFileAttributes.class);
    return new UfsFileStatus(path, file.length(), file.lastModified(), attr.owner().getName(),
        attr.group().getName(), FileUtils.translatePosixPermissionToMode(attr.permissions()));
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
        throw new IOException("Unknown space type: " + type);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.isDirectory();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    return file.isFile();
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    File[] files = file.listFiles();
    if (files != null) {
      UfsStatus[] rtn = new UfsStatus[files.length];
      int i = 0;
      for (File f : files) {
        // TODO(adit): do we need extra call for attributes?
        PosixFileAttributes attr =
            Files.readAttributes(Paths.get(f.getPath()), PosixFileAttributes.class);
        short mode = FileUtils.translatePosixPermissionToMode(attr.permissions());
        UfsStatus retStatus;
        if (f.isDirectory()) {
          retStatus = new UfsDirectoryStatus(f.getName(), attr.owner().getName(),
              attr.group().getName(), mode);
        } else {
          retStatus = new UfsFileStatus(f.getName(), f.length(), f.lastModified(),
              attr.owner().getName(), attr.group().getName(), mode);
        }
        rtn[i++] = retStatus;
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    path = stripPath(path);
    File file = new File(path);
    if (!options.getCreateParent()) {
      if (file.mkdir()) {
        setMode(file.getPath(), options.getMode().toShort());
        FileUtils.setLocalDirStickyBit(file.getPath());
        try {
          setOwner(file.getPath(), options.getOwner(), options.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update the ufs dir ownership, default values will be used: {}",
              e.getMessage());
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
        setMode(dirToMake.getAbsolutePath(), options.getMode().toShort());
        FileUtils.setLocalDirStickyBit(file.getPath());
        // Set the owner to the Alluxio client user to achieve permission delegation.
        // Alluxio server-side user is required to be a superuser. If it fails to set owner,
        // proceeds with mkdirs and print out an warning message.
        try {
          setOwner(dirToMake.getAbsolutePath(), options.getOwner(), options.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update the ufs dir ownership, default values will be used: {}",
              e.getMessage());
        }
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    path = stripPath(path);
    FileInputStream inputStream = new FileInputStream(path);
    try {
      ByteStreams.skipFully(inputStream, options.getOffset());
    } catch (IOException e) {
      inputStream.close();
      throw e;
    }
    return inputStream;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    if (!isDirectory(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file.", src, dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a directory.", src,
          dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    path = stripPath(path);
    try {
      if (!Strings.isNullOrEmpty(user)) {
        FileUtils.changeLocalFileUser(path, user);
      }
      if (!Strings.isNullOrEmpty(group)) {
        FileUtils.changeLocalFileGroup(path, group);
      }
    } catch (IOException e) {
      LOG.warn("Failed to set owner for {} with user: {}, group: {}", path, user, group);
      LOG.debug("Exception: ", e);
      LOG.warn("In order for Alluxio to modify ownership of local files, "
          + "Alluxio should be the local file system superuser.");
      if (!Configuration.getBoolean(PropertyKey.UNDERFS_ALLOW_SET_OWNER_FAILURE)) {
        throw e;
      } else {
        LOG.warn("Failure is ignored, which may cause permission inconsistency between "
            + "Alluxio and local under file system.");
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
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  /**
   * Rename a file to a file or a directory to a directory.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    src = stripPath(src);
    dst = stripPath(dst);
    File file = new File(src);
    return file.renameTo(new File(dst));
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }
}
