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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import alluxio.perf.PerfConstants;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;

/**
 * The interface layer to communicate with Alluxio. Now Alluxio Client APIs may change and this
 * layer can keep the modifications of Alluxio-Perf in this single file.
 */
public class AlluxioPerfFS implements PerfFS {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  public static AlluxioPerfFS get() throws IOException {
    return new AlluxioPerfFS();
  }

  private FileSystem mFs;

  private AlluxioPerfFS() {
    mFs = FileSystem.Factory.get();
  }

  /**
   * Close the connection to Alluxio
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {}

  /**
   * Create a file. Use the default block size and TRY_CACHE write type.
   *
   * @param path the file's full path
   * @return the output stream of the created file
   * @throws IOException
   */
  @Override
  public OutputStream create(String path) throws IOException {
    long size = new Configuration().getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
    return create(path, (int) size);
  }

  /**
   * Create a file with the specified block size. Use the MUST_CACHE write type.
   *
   * @param path the file's full path
   * @param blockSizeByte the block size of the file
   * @return the output stream of the created file
   * @throws IOException
   */
  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path, blockSizeByte, "MUST_CACHE");
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
    WriteType type = WriteType.valueOf(writeType);
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.createFile(uri,
          CreateFileOptions.defaults().setBlockSizeBytes(blockSizeByte).setWriteType(type));
    } catch (FileAlreadyExistsException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (mFs.exists(uri)) {
        return false;
      }
      return (mFs.createFile(uri) != null);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
    try {
      mFs.delete(new AlluxioURI(path), options);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    return true;
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
    try {
      return mFs.exists(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (!mFs.exists(uri)) {
        return 0;
      }
      return mFs.getStatus(uri).getLength();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    AlluxioURI uri = new AlluxioURI(path);
    try {
      if (!mFs.exists(uri)) {
        return false;
      }
      return mFs.getStatus(uri).isFolder();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    return !isDirectory(path);
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
    List<URIStatus> files;
    try {
      files = mFs.listStatus(new AlluxioURI(path));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    if (files == null) {
      return null;
    }
    ArrayList<String> ret = new ArrayList<String>(files.size());
    for (URIStatus fileInfo : files) {
      ret.add(fileInfo.getPath());
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
    AlluxioURI uri = new AlluxioURI(path);
    try {
      mFs.createDirectory(uri, CreateDirectoryOptions.defaults().setRecursive(createParent));
      return true;
    } catch (AlluxioException e) {
      return false;
    }
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
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.openFile(uri);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    ReadType type = ReadType.valueOf(readType);
    AlluxioURI uri = new AlluxioURI(path);
    try {
      return mFs.openFile(uri, OpenFileOptions.defaults().setReadType(type));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
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
    AlluxioURI srcURI = new AlluxioURI(src);
    AlluxioURI dstURI = new AlluxioURI(dst);
    try {
      mFs.rename(srcURI, dstURI);
      return true;
    } catch (AlluxioException e) {
      return false;
    }
  }
}
