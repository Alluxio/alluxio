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

package alluxio.underfs.local;

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

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

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

  /**
   * Constructs a new {@link LocalUnderFileSystem}.
   *
   * @param conf the configuration for Alluxio
   */
  public LocalUnderFileSystem(Configuration conf) {
    super(conf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.LOCAL;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public OutputStream create(String path) throws IOException {
    FileOutputStream stream = new FileOutputStream(path);
    try {
      setPermission(path, "777");
    } catch (IOException e) {
      stream.close();
      throw e;
    }
    return stream;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path, (short) 1, blockSizeByte);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    if (replication != 1) {
      throw new IOException("UnderFileSystemSingleLocal does not provide more than one"
          + " replication factor");
    }
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    File file = new File(path);
    boolean success = true;
    if (recursive && file.isDirectory()) {
      String[] files = file.list();
      for (String child : files) {
        success = success && delete(PathUtils.concatPath(path, child), true);
      }
    }

    return success && file.delete();
  }

  @Override
  public boolean exists(String path) throws IOException {
    File file = new File(path);
    return file.exists();
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    File file = new File(path);
    if (!file.exists()) {
      throw new FileNotFoundException(path);
    }
    return Constants.GB * 2L;
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<String>();
    ret.add(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mConfiguration));
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    return getFileLocations(path);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    File file = new File(path);
    return file.length();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    File file = new File(path);
    return file.lastModified();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
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
    File file = new File(path);
    return file.isFile();
  }

  @Override
  public String[] list(String path) throws IOException {
    File file = new File(path);
    File[] files = file.listFiles();
    if (files != null) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (File f : files) {
        rtn[i ++] = f.getName();
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    File file = new File(path);
    if (!createParent) {
      if (file.mkdir()) {
        setPermission(file.getPath(), "777");
        FileUtils.setLocalDirStickyBit(file.getPath());
        return true;
      }
      return false;
    }
    // create parent directories one by one and set their permissions to 777
    Stack<File> dirsToMake = new Stack<File>();
    dirsToMake.push(file);
    File parent = file.getParentFile();
    while (!parent.exists()) {
      dirsToMake.push(parent);
      parent = parent.getParentFile();
    }
    while (!dirsToMake.empty()) {
      File dirToMake = dirsToMake.pop();
      if (dirToMake.mkdir()) {
        setPermission(dirToMake.getAbsolutePath(), "777");
        FileUtils.setLocalDirStickyBit(file.getPath());
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return new FileInputStream(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    File file = new File(src);
    return file.renameTo(new File(dst));
  }

  @Override
  public void setConf(Object conf) {}

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
    FileUtils.changeLocalFilePermission(path, posixPerm);
  }

  @Override
  public void connectFromMaster(Configuration conf, String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(Configuration conf, String hostname) throws IOException {
    // No-op
  }
}
