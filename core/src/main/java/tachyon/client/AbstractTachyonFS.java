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

package tachyon.client;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * Abstract implementation of {@link tachyon.client.TachyonFSCore} APIs.
 */
abstract class AbstractTachyonFS implements TachyonFSCore {
  protected final TachyonConf mTachyonConf;

  protected AbstractTachyonFS(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
  }

  /**
   * Creates a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   * 
   * @param path the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path) throws IOException {
    long defaultBlockSize = mTachyonConf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE,
        Constants.DEFAULT_BLOCK_SIZE_BYTE);
    return createFile(path, defaultBlockSize);
  }

  /**
   * Creates a file in the system. It also creates necessary folders along the path. // TODO It
   * should not create necessary path.
   * 
   * @param path the path of the file
   * @param blockSizeByte the block size of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    return createFile(path, TachyonURI.EMPTY_URI, blockSizeByte, true);
  }

  /**
   * Creates a file in the system with a pre-defined underfsPath. It also creates necessary folders
   * along the path. // TODO It should not create necessary path.
   * 
   * @param path the path of the file in Tachyon
   * @param ufsPath the path of the file in the underfs
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException If file already exists, or path is invalid.
   */
  public synchronized int createFile(TachyonURI path, TachyonURI ufsPath) throws IOException {
    return createFile(path, ufsPath, -1, true);
  }

  /**
   * Deletes the file denoted by the file id.
   * 
   * @param fid file id
   * @param recursive if delete the path recursively.
   * @return true if deletion succeed (including the case the file does not exist in the first
   *         place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean delete(int fid, boolean recursive) throws IOException {
    return delete(fid, TachyonURI.EMPTY_URI, recursive);
  }

  /**
   * Deletes the file denoted by the path.
   * 
   * @param path the file path
   * @param recursive if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean delete(TachyonURI path, boolean recursive) throws IOException {
    return delete(-1, path, recursive);
  }

  /**
   * Create a directory if it does not exist. The method also creates necessary non-existing parent
   * folders.
   * 
   * @param path Directory path.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  public synchronized boolean mkdir(TachyonURI path) throws IOException {
    return mkdirs(path, true);
  }

  /**
   * Renames the file
   * 
   * @param fileId the file id
   * @param dstPath the new path of the file in the file system.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean rename(int fileId, TachyonURI dstPath) throws IOException {
    return rename(fileId, TachyonURI.EMPTY_URI, dstPath);
  }

  /**
   * Rename the srcPath to the dstPath
   * 
   * @param srcPath The path of the source file / folder.
   * @param dstPath The path of the destination file / folder.
   * @return true if succeed, false otherwise.
   * @throws IOException
   */
  public synchronized boolean rename(TachyonURI srcPath, TachyonURI dstPath) throws IOException {
    return rename(-1, srcPath, dstPath);
  }

  /**
   * Frees the in-memory blocks of file/folder denoted by the path
   * 
   * @param path the file/folder path
   * @param recursive if free the path recursively
   * @return true if the memory free succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean freepath(TachyonURI path, boolean recursive) throws IOException {
    return freepath(-1, path, recursive);
  }

  /**
   * Set owner of a fileId(i.e. a file or a directory).
   * The parameters username and groupname cannot both be null
   * @param fileId the file id
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * @param recursive If fileId represents a folder, change the folder owner recursively
   * @return true if setOwner successfully, false otherwise.
   * @throws IOException
   */
  public synchronized boolean setOwner(int fileId, String username, String groupname,
      boolean recursive) throws IOException {
    return setOwner(fileId, TachyonURI.EMPTY_URI, username, groupname, recursive);
  }

  /**
   * Set owner of a path(i.e. a file or a directory).
   * The parameters username and groupname cannot both be null
   * @param fileId the file id
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * @param recursive If path represents a folder, change the folder owner recursively
   * @return true if setOwner successfully, false otherwise.
   * @throws IOException
   */
  public synchronized boolean setOwner(TachyonURI path, String username, String groupname,
      boolean recursive) throws IOException {
    return setOwner(-1, path, username, groupname, recursive);
  }

  /**
   * Set permission of a fildId(i.e. a file or a directory)
   * 
   * @param fileId The id of the file / folder
   * @param short permission, e.g. 777
   * @param recursive If fileId represents a folder, change the folder permission recursively
   * @return true if setPermission successfully, false otherwise.
   * @throws IOException
   */
  public synchronized boolean setPermission(int fileId, short permission, boolean recursive)
      throws IOException {
    return setPermission(fileId, TachyonURI.EMPTY_URI, permission, recursive);
  }

  /**
   * Set permission of a path(i.e. a file or a directory)
   * 
   * @param path The path of the file / folder
   * @param short permission, e.g. 777
   * @param recursive If path represents a folder, change the folder permission recursively
   * @return true if setPermission successfully, false otherwise.
   * @throws IOException
   */
  public synchronized boolean setPermission(TachyonURI path, short permission, boolean recursive)
      throws IOException {
    return setPermission(-1, path, permission, recursive);
  }
}
