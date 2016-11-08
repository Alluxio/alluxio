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

package alluxio.underfs.swift;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * OpenStack Swift API {@link UnderFileSystem} implementation based on the Stocator library.
 */
@ThreadSafe
public class StocatorUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Stocator access. */
  private final FileSystem mFileSystem;

  /**
   * Constructs a new Swift {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public StocatorUnderFileSystem(AlluxioURI uri) {
    super(uri);
    LOG.debug("Stocator under fs constructor {}", uri.toString());
    org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();
    hConf.set("fs.swift2d.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem");
    hConf.set("fs.swift2d.service.srv.auth.url", Configuration.get(PropertyKey.SWIFT_AUTH_URL_KEY));
    hConf.set("fs.swift2d.service.srv.public",
        Configuration.get(PropertyKey.SWIFT_USE_PUBLIC_URI_KEY));
    hConf.set("fs.swift2d.service.srv.tenant", Configuration.get(PropertyKey.SWIFT_TENANT_KEY));
    hConf.set("fs.swift2d.service.srv.password", Configuration.get(PropertyKey.SWIFT_PASSWORD_KEY));
    hConf.set("fs.swift2d.service.srv.username", Configuration.get(PropertyKey.SWIFT_USER_KEY));
    hConf.set("fs.swift2d.service.srv.auth.method",
        Configuration.get(PropertyKey.SWIFT_AUTH_METHOD_KEY));
    LOG.debug("Stocator under fs init {}", uri.toString());
    try {
      mFileSystem = FileSystem.get(new URI(uri.toString()) , hConf);
      LOG.debug("Stocator under fs init successfull {}", uri.toString());
    } catch (IOException | URISyntaxException e) {
      LOG.error("Exception thrown when trying to get FileSystem for {}", uri.toString(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    LOG.debug("close");
    mFileSystem.close();
  }

  @Override
  public void connectFromMaster(String hostname) {
    LOG.debug("connect from master");
  }

  @Override
  public void connectFromWorker(String hostname) {
    LOG.debug("connect from worker");
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    return create(path, new CreateOptions());
  }

  @Override
  public FSDataOutputStream create(String path, CreateOptions options)
      throws IOException {
    try {
      return FileSystem.create(mFileSystem, new Path(path), null);
    }  catch (IOException e) {
      LOG.error("Failed to create {}", path);
      throw e;
    }
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("Delete method: {}, recursive {}", path, recursive);
    return mFileSystem.delete(new Path(path), recursive);
  }

  @Override
  public boolean exists(String path) throws IOException {
    LOG.debug("Exists {}", path);
    return mFileSystem.exists(new Path(path));
  }

  /**
   * Gets the block size in bytes. There is no concept of a block in Swift and the maximum size of
   * one file is 4 GB. This method defaults to the default user block size in Alluxio.
   *
   * @param path the path to the object
   * @return the default Alluxio user block size
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public Object getConf() {
    LOG.debug("getConf is not supported when using StocatorUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using "
        + "StocatorUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.debug("getFileLocations is not supported when using "
        + "StocatorUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    Path tPath = new Path(path);
    try {
      FileStatus fs = mFileSystem.getFileStatus(tPath);
      return fs.getLen();
    } catch (IOException e) {
      LOG.error("Error fetching file size, assuming file does not exist", e);
      throw new FileNotFoundException(path);
    }
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFileSystem.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return fs.getModificationTime();
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mFileSystem.isFile(new Path(path));
  }

  @Override
  public String[] list(String path) throws IOException {
    FileStatus[] files;
    try {
      files = mFileSystem.listStatus(new Path(path));
    } catch (FileNotFoundException e) {
      return null;
    }
    if (files != null && !isFile(path)) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        // only return the relative path, to keep consistent with java.io.File.list()
        rtn[i++] = status.getPath().getName();
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
    return mFileSystem.mkdirs(new Path(path));
  }

  @Override
  public FSDataInputStream open(String path) throws IOException {
    LOG.debug("Open file {}", path);
    try {
      FSDataInputStream in = mFileSystem.open(new Path(path));
      LOG.debug("Got input stream for {}", path);
      return in;
    } catch (IOException e) {
      LOG.error("Failed to open {} : {}", path, e.getMessage(), e);
      throw e;
    }
  }

  /**
   * @inheritDoc
   * Rename will overwrite destination if it already exists
   *
   * @param source the source file or folder name
   * @param destination the destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  @Override
  public boolean rename(String source, String destination) throws IOException {
    return mFileSystem.rename(new Path(source), new Path(destination));
  }

  @Override
  public void setConf(Object conf) {}

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  // No ACL integration currently, returns default empty value
  @Override
  public String getOwner(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default empty value
  @Override
  public String getGroup(String path) throws IOException {
    return "";
  }

  // No ACL integration currently, returns default value
  @Override
  public short getMode(String path) throws IOException {
    return Constants.DEFAULT_FILE_SYSTEM_MODE;
  }

  @Override
  public String getUnderFSType() {
    return "swift2d";
  }
}
