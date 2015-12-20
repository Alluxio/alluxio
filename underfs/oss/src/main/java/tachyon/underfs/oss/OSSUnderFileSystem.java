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

package tachyon.underfs.oss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import tachyon.Constants;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

/**
 * Under file system implementation for Aliyun OSS using the oss SDK library.
 */
public class OSSUnderFileSystem extends UnderFileSystem {
  //private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Suffix for an empty file to flag it as a directory */
  //private static final String FOLDER_SUFFIX = "_$folder$";
  /** Value used to indicate folder structure in S3 */
  //private static final String PATH_SEPARATOR = "/";
  /** Aliyun OSS client */
  // TODO(luoli523523) include OSS dependency late
  //private OSSClient mClient;
  /** Bucket name of user's configured Tachyon bucket */
  //private final String mBucketName;
  /** Prefix of the bucket, for example s3n://my-bucket-name/ */
  //private final String mBucketPrefix;

  protected OSSUnderFileSystem(String bucketName, TachyonConf tachyonConf) throws Exception {
    super(tachyonConf);
    // TODO(luoli523523) Auto-generated constructor stub
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.OSS;
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) throws IOException {
    // TODO(luoli523) Auto-generated method stub
  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) throws IOException {
    // TODO(luoli523) Auto-generated method stub
  }

  @Override
  public void close() throws IOException {
    // TODO(luoli523) Auto-generated method stub
  }

  @Override
  public OutputStream create(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return 0;
  }

  @Override
  public Object getConf() {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return 0;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return 0;
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return false;
  }

  @Override
  public String[] list(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return null;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    // TODO(luoli523) Auto-generated method stub
    return false;
  }

  @Override
  public void setConf(Object conf) {
    // TODO(luoli523) Auto-generated method stub
  }

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {
    // TODO(luoli523) Auto-generated method stub
  }

}
