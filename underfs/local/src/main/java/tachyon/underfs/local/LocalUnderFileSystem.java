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

package tachyon.underfs.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import tachyon.conf.TachyonConf;
import tachyon.Constants;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;

/**
 * Local node UnderFilesystem implementation.
 * <p>
 * This is primarily intended for local unit test and single machine mode. In principal it can also
 * be used on a system where a shared file system (e.g. NFS) is mounted at the same path on every
 * node of the system. However it is generally preferable to use a proper distributed file system
 * for that scenario.
 * </p>
 */
public class LocalUnderFileSystem extends UnderFileSystem {

  public LocalUnderFileSystem(TachyonConf conf) {
    super(conf);
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
    CommonUtils.setLocalFileStickyBit(path);
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
        success = success && delete(CommonUtils.concatPath(path, child), true);
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
    ret.add(NetworkUtils.getLocalHostName(mTachyonConf));
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
    boolean created = createParent ? file.mkdirs() : file.mkdir();
    setPermission(path, "777");
    CommonUtils.setLocalFileStickyBit(path);
    return created;
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
    CommonUtils.changeLocalFilePermission(path, posixPerm);
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) throws IOException {
    // No-op
  }
}
