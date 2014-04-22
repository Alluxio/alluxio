/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.glusterfs.*;

import tachyon.conf.CommonConf;
import tachyon.util.CommonUtils;

/**
 * HDFS UnderFilesystem implementation.
 */
public class UnderFileSystemHdfs extends UnderFileSystem {
  private static final int MAX_TRY = 5;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private FileSystem mFs = null;
  private String mUfsPrefix = null;
  // TODO add sticky bit and narrow down the permission in hadoop 2
  private static final FsPermission PERMISSION = new FsPermission((short) 0777)
      .applyUMask(FsPermission.createImmutable((short) 0000));

  public static UnderFileSystemHdfs getClient(String path) {
    return new UnderFileSystemHdfs(path);
  }

  private UnderFileSystemHdfs(String fsDefaultName) {
    try {
      mUfsPrefix = fsDefaultName;
      Configuration tConf = new Configuration();
      tConf.set("fs.defaultFS", fsDefaultName);
      String glusterfsPrefix = "glusterfs:///";
      if (fsDefaultName.startsWith(glusterfsPrefix)) {
          tConf.set("fs.glusterfs.impl", CommonConf.get().UNDERFS_GLUSTERFS_IMPL);
       	  tConf.set("mapred.system.dir", CommonConf.get().UNDERFS_GLUSTERFS_MR_DIR);
          tConf.set("fs.glusterfs.volumes", CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES);
   		  tConf.set("fs.glusterfs.volume.fuse." + CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES, 
   				  CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS);
      }else{
          tConf.set("fs.hdfs.impl", CommonConf.get().UNDERFS_HDFS_IMPL);

          // To disable the instance cache for hdfs client, otherwise it causes the
          // FileSystem closed exception. Being configurable for unit/integration
          // test only, and not expose to the end-user currently.
          tConf.set("fs.hdfs.impl.disable.cache",
                    System.getProperty("fs.hdfs.impl.disable.cache", "false"));
      }

      if (System.getProperty("fs.s3n.awsAccessKeyId") != null) {
          tConf.set("fs.s3n.awsAccessKeyId", System.getProperty("fs.s3n.awsAccessKeyId"));
      }
      if (System.getProperty("fs.s3n.awsSecretAccessKey") != null) {
          tConf.set("fs.s3n.awsSecretAccessKey", System.getProperty("fs.s3n.awsSecretAccessKey"));
      }
      Path path = new Path(fsDefaultName);
      mFs = path.getFileSystem(tConf);
      // FileSystem.get(tConf);
      // mFs = FileSystem.get(new URI(fsDefaultName), tConf);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    mFs.close();
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        LOG.debug("Creating HDFS file at " + path);
        return FileSystem.create(mFs, new Path(path), PERMISSION);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    throw te;
  }

  @Override
  // BlockSize should be a multiple of 512
      public
      FSDataOutputStream create(String path, int blockSizeByte) throws IOException {
    // TODO Fix this
    // return create(path, (short) Math.min(3, mFs.getDefaultReplication()), blockSizeByte);
    return create(path);
  }

  @Override
  public FSDataOutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    // TODO Fix this
    // return create(path, (short) Math.min(3, mFs.getDefaultReplication()), blockSizeByte);
    return create(path);
    // LOG.info(path + " " + replication + " " + blockSizeByte);
    // IOException te = null;
    // int cnt = 0;
    // while (cnt < MAX_TRY) {
    // try {
    // return mFs.create(new Path(path), true, 4096, replication, blockSizeByte);
    // } catch (IOException e) {
    // cnt ++;
    // LOG.error(cnt + " : " + e.getMessage(), e);
    // te = e;
    // continue;
    // }
    // }
    // throw te;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("deleting " + path + " " + recursive);
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.delete(new Path(path), recursive);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.exists(new Path(path));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFs.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFs.getFileStatus(tPath);
    return fs.getBlockSize();
  }

  @Override
  public List<String> getFileLocations(String path) {
    return getFileLocations(path, 0);
  }

  @Override
  public List<String> getFileLocations(String path, long offset) {
    List<String> ret = new ArrayList<String>();
    try {
      FileStatus fStatus = mFs.getFileStatus(new Path(path));
      BlockLocation[] bLocations = mFs.getFileBlockLocations(fStatus, offset, 1);
      if (bLocations.length > 0) {
        String[] hosts = bLocations[0].getHosts();
        Collections.addAll(ret, hosts);
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    return ret;
  }

  @Override
  public long getFileSize(String path) {
    int cnt = 0;
    Path tPath = new Path(path);
    while (cnt < MAX_TRY) {
      try {
        FileStatus fs = mFs.getFileStatus(tPath);
        return fs.getLen();
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        continue;
      }
    }
    return -1;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFs.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFs.getFileStatus(tPath);
    return fs.getModificationTime();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // Ignoring the path given, will give information for entire cluster
    // as Tachyon can load/store data out of entire HDFS cluster
    if (mFs instanceof DistributedFileSystem) {
      switch (type) {
      case SPACE_TOTAL:
        return ((DistributedFileSystem) mFs).getDiskStatus().getCapacity();
      case SPACE_USED:
        return ((DistributedFileSystem) mFs).getDiskStatus().getDfsUsed();
      case SPACE_FREE:
        return ((DistributedFileSystem) mFs).getDiskStatus().getRemaining();
      }
    }
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mFs.isFile(new Path(path));
  }

  @Override
  public String[] list(String path) throws IOException {
    FileStatus[] files = mFs.listStatus(new Path(path));
    if (files != null) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        rtn[i ++] = status.getPath().toString().substring(mUfsPrefix.length());
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        if (mFs.exists(new Path(path))) {
          return true;
        }
        return mFs.mkdirs(new Path(path), PERMISSION);
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }

  @Override
  public FSDataInputStream open(String path) {
    IOException te = null;
    int cnt = 0;
    while (cnt < MAX_TRY) {
      try {
        return mFs.open(new Path(path));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return null;
  }

  @Override
  public boolean rename(String src, String dst) {
    IOException te = null;
    int cnt = 0;
    LOG.debug("Renaming from " + src + " to " + dst);
    if (!exists(src)) {
      LOG.error("File " + src + " does not exist. Therefore rename to " + dst + " failed.");
    }

    if (exists(dst)) {
      LOG.error("File " + dst + " does exist. Therefore rename from " + src + " failed.");
    }

    while (cnt < MAX_TRY) {
      try {
        return mFs.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        cnt ++;
        LOG.error(cnt + " : " + e.getMessage(), e);
        te = e;
        continue;
      }
    }
    CommonUtils.runtimeException(te);
    return false;
  }

  @Override
  public void toFullPermission(String path) {
    try {
      FileStatus fileStatus = mFs.getFileStatus(new Path(path));
      LOG.info("Changing file '" + fileStatus.getPath() + "' permissions from: "
          + fileStatus.getPermission() + " to 777");
      mFs.setPermission(fileStatus.getPath(), PERMISSION);
    } catch (IOException e) {
      LOG.error(e);
    }
  }
}
