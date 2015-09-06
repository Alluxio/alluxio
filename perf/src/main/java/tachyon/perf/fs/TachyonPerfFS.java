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

package tachyon.perf.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.perf.PerfConstants;
import tachyon.thrift.ClientFileInfo;

/**
 * The interface layer to communicate with Tachyon. Now Tachyon Client APIs may change and this
 * layer can keep the modifications of Tachyon-Perf in this single file.
 */
public class TachyonPerfFS implements PerfFS {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static TachyonPerfFS get() throws IOException {
    return new TachyonPerfFS();
  }

  private TachyonConf mTachyonConf;
  private TachyonFS mTfs;

  private TachyonPerfFS() {
    mTachyonConf = new TachyonConf();
    mTfs = TachyonFS.get(mTachyonConf);
  }

  /**
   * Close the connection to Tachyon
   *
   * @throws IOException
   */
  public void close() throws IOException {
    mTfs.close();
  }

  /**
   * Create a file. Use the default block size and TRY_CACHE write type.
   *
   * @param path the file's full path
   * @return the output stream of the created file
   * @throws IOException
   */
  public OutputStream create(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri);
    }
    return mTfs.getFile(uri).getOutStream(WriteType.TRY_CACHE);
  }

  /**
   * Create a file with the specified block size. Use the TRY_CACHE write type.
   *
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri, blockSizeByte);
    }
    return mTfs.getFile(uri).getOutStream(WriteType.TRY_CACHE);
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
  public OutputStream create(String path, int blockSizeByte, String writeType) throws IOException {
    WriteType type = WriteType.valueOf(writeType);
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      mTfs.createFile(uri, blockSizeByte);
    }
    return mTfs.getFile(uri).getOutStream(type);
  }

  /**
   * Create an empty file
   *
   * @param path the file's full path
   * @return true if success, false otherwise.
   * @throws IOException
   */
  public boolean createEmptyFile(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (mTfs.exist(uri)) {
      return false;
    }
    return (mTfs.createFile(uri) != -1);
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
  public boolean delete(String path, boolean recursive) throws IOException {
    return mTfs.delete(new TachyonURI(path), recursive);
  }

  /**
   * Check whether the file exists or not.
   *
   * @param path the file's full path
   * @return true if exists, false otherwise
   * @throws IOException
   */
  public boolean exists(String path) throws IOException {
    return mTfs.exist(new TachyonURI(path));
  }

  /**
   * Get the length of the file, in bytes.
   *
   * @param path
   * @return the length of the file in bytes
   * @throws IOException
   */
  public long getLength(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return 0;
    }
    return mTfs.getFile(uri).length();
  }

  /**
   * Check if the path is a directory.
   *
   * @param path the file's full path
   * @return true if it's a directory, false otherwise
   * @throws IOException
   */
  public boolean isDirectory(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return false;
    }
    return mTfs.getFile(uri).isDirectory();
  }

  /**
   * Check if the path is a file.
   *
   * @param path the file's full path
   * @return true if it's a file, false otherwise
   * @throws IOException
   */
  public boolean isFile(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      return false;
    }
    return mTfs.getFile(uri).isFile();
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
  public List<String> listFullPath(String path) throws IOException {
    List<ClientFileInfo> files = mTfs.listStatus(new TachyonURI(path));
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (ClientFileInfo fileInfo : files) {
      ret.add(fileInfo.path);
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
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (mTfs.exist(uri)) {
      return false;
    }
    return mTfs.mkdirs(uri, createParent);
  }

  /**
   * Open a file and return it's input stream. Use the NO_CACHE read type.
   *
   * @param path the file's full path
   * @return the input stream of the opened file
   * @throws IOException
   */
  public InputStream open(String path) throws IOException {
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.getFile(uri).getInStream(ReadType.NO_CACHE);
  }

  /**
   * Open a file and return it's input stream, with the specified read type.
   *
   * @param path the file's full path
   * @param readType the read type of the file
   * @return the input stream of the opened file
   * @throws IOException
   */
  public InputStream open(String path, String readType) throws IOException {
    ReadType type = ReadType.valueOf(readType);
    TachyonURI uri = new TachyonURI(path);
    if (!mTfs.exist(uri)) {
      throw new FileNotFoundException("File not exists " + path);
    }
    return mTfs.getFile(uri).getInStream(type);
  }

  /**
   * Rename the file.
   *
   * @param src the src full path
   * @param dst the dst full path
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean rename(String src, String dst) throws IOException {
    TachyonURI srcURI = new TachyonURI(src);
    TachyonURI dstURI = new TachyonURI(dst);
    return mTfs.rename(srcURI, dstURI);
  }
}
