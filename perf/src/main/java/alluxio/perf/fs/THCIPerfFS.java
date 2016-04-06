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

package alluxio.perf.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;

import alluxio.perf.PerfConstants;

import alluxio.Constants;
import alluxio.client.file.BaseFileSystem;

public class THCIPerfFS implements PerfFS {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static THCIPerfFS get() throws IOException {
    return new THCIPerfFS();
  }

  private alluxio.Configuration mAlluxioConf;
  private FileSystem mTfs;

  private THCIPerfFS() {
    try {
      mAlluxioConf = new alluxio.Configuration();
      Configuration conf = new Configuration();
      conf.set("fs.alluxio.impl", BaseFileSystem.class.getName());
      String masterAddr = mAlluxioConf.get(Constants.MASTER_ADDRESS);
      URI u = new URI(masterAddr);
      mTfs = FileSystem.get(u, conf);
    } catch (IOException e) {
      LOG.error("Failed to get AlluxioFS", e);
      Throwables.propagate(e);
    } catch (URISyntaxException u) {
      LOG.error("Failed to parse underfs uri", u);
      Throwables.propagate(u);
    }
  }

  /**
   * Close the connection to Alluxio
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    mTfs.close();
  }

  /**
   * Create a file. Use the default block size and TRY_CACHE write type.
   *
   * @param  path the file's full cPath
   * @return the output stream of the created file
   * @throws IOException
   */
  @Override
  public OutputStream create(String path) throws IOException {
    Path p = new Path(path);
    return mTfs.create(p);
  }

  /**
   * Create a file with the specified block size. Use the TRY_CACHE write type.
   *
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    Path p = new Path(path);
    return mTfs.create(p);
  }

  /**
   * Create a file with the specified block size and write type.
   *
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @param writeType the write type of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  @Override
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    // Write type needs to be set with Alluxio Java option in alluxio-perf-env.sh
    Path p = new Path(path);
    return mTfs.create(p);
  }

  /**
   * Create an empty file
   *
   * @param path the file's full path
   * @return true if success, false otherwise.
   * @throws IOException
   */
  @Override
  public boolean createEmptyFile(String path) throws IOException {
    Path p = new Path(path);
    if (mTfs.exists(p)) {
      return false;
    }
    mTfs.create(p).close();
    return true;
  }

  /**
   * Delete the file. If recursive and the path is a directory, it will delete all the files under
   * the path.
   *
   * @param path the file's full path
   * @param recursive It true, delete recursively
   * @return true if success, false otherwise.
   * @throws IOException
   */
  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    return mTfs.delete(new Path(path), recursive);
  }

  /**
   * Check whether the file exists or not.
   *
   * @param path the file's full path
   * @return true if exists, false otherwise
   * @throws IOException
   */
  @Override
  public boolean exists(String path) throws IOException {
    return mTfs.exists(new Path(path));
  }

  /**
   * Get the length of the file, in bytes.
   *
   * @param path
   * @return the length of the file in bytes
   * @throws IOException
   */
  @Override
  public long getLength(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return 0;
    }
    return mTfs.getFileStatus(p).getLen();
  }

  /**
   * Check if the path is a directory.
   *
   * @param path the file's full path
   * @return true if it's a directory, false otherwise
   * @throws IOException
   */
  @Override
  public boolean isDirectory(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return false;
    }
    return mTfs.getFileStatus(p).isDir();
  }

  /**
   * Check if the path is a file.
   *
   * @param path the file's full path
   * @return true if it's a file, false otherwise
   * @throws IOException
   */
  @Override
  public boolean isFile(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      return false;
    }
    return !mTfs.getFileStatus(p).isDir();
  }

  /**
   * List the files under the path. If the path is a file, return the full path of the file. if the
   * path is a directory, return the full paths of all the files under the path. Otherwise return
   * null.
   *
   * @param path the file's full path
   * @return the list contains the full paths of the listed files
   * @throws IOException
   */
  @Override
  public List<String> listFullPath(String path) throws IOException {
    List<FileStatus> files = Arrays.asList(mTfs.listStatus(new Path(path)));
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (FileStatus fileInfo : files) {
      ret.add(fileInfo.getPath().toString());
    }
    return ret;
  }

  /**
   * Creates the directory named by the path. If the folder already exists, the method returns
   * false.
   *
   * @param path the file's full path
   * @param createParent If true, the method creates any necessary but nonexistent parent
   *        directories. Otherwise, the method does not create nonexistent parent directories.
   * @return true if success, false otherwise
   * @throws IOException
   */
  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    Path p = new Path(path);
    if (mTfs.exists(p)) {
      return false;
    }
    return mTfs.mkdirs(p);
  }

  /**
   * Open a file and return it's input stream. Use the NO_CACHE read type.
   *
   * @param path the file's full path
   * @return the input stream of the opened file
   * @throws IOException
   */
  @Override
  public InputStream open(String path) throws IOException {
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.open(p);
  }

  /**
   * Open a file and return it's input stream, with the specified read type.
   *
   * @param path the file's full path
   * @param readType the read type of the file
   * @return the input stream of the opened file
   * @throws IOException
   */
  @Override
  public InputStream open(String path, String readType) throws IOException {
    // Read type hardcoded in thci
    Path p = new Path(path);
    if (!mTfs.exists(p)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.open(p);
  }

  /**
   * Rename the file.
   *
   * @param src the src full path
   * @param dst the dst full path
   * @return true if success, false otherwise
   * @throws IOException
   */
  @Override
  public boolean rename(String src, String dst) throws IOException {
    Path srcPath = new Path(src);
    Path dstPath = new Path(dst);
    return mTfs.rename(srcPath, dstPath);
  }
}
