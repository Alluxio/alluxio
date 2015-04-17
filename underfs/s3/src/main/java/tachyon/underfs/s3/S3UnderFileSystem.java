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

package tachyon.underfs.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

/**
 * Under file system implementation for S3 using the Jets3t library.
 */
public class S3UnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final S3Service mClient;
  private final S3Bucket mBucket;

  public S3UnderFileSystem(String bucketName, TachyonConf tachyonConf) {
    super(tachyonConf);
    AWSCredentials awsCredentials =
        new AWSCredentials(tachyonConf.get("fs.s3n.awsAccessKeyId", null), tachyonConf.get(
            "fs.s3n.awsSecretAccessKey", null));
    mClient = new RestS3Service(awsCredentials);
    mBucket = new S3Bucket(bucketName);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(TachyonConf conf, String hostname) {

  }

  @Override
  public void connectFromWorker(TachyonConf conf, String hostname) {

  }

  @Override
  public OutputStream create(String path) throws IOException {
    return null;
  }

  // Not supported
  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path);
  }

  // Not supported
  @Override
  public OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    if (!recursive) {
      return delete(path);
    }

    // Get all relevant files
    String[] pathsToDelete = list(path);
    for (String pathToDelete : pathsToDelete) {
      // If we fail to delete one file, stop
      if (!delete(pathToDelete)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    try {
      mClient.getObjectDetails(mBucket, path);
      return true;
    } catch (ServiceException s) {
      return false;
    }
  }

  // Largest size of one file is 5 TB
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Constants.TB * 5;
  }

  // Not supported
  @Override
  public Object getConf() {
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    return getObjectDetails(path).getContentLength();
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    return getObjectDetails(path).getLastModifiedDate().getTime();
  }

  // S3 is not really bounded
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    switch (type) {
      // TODO: This might be really slow
      case SPACE_USED:
        long ret = 0L;
        String[] keys = list(path);
        for (String key : keys) {
          ret += getObjectDetails(key).getContentLength();
        }
        return ret;
      case SPACE_FREE:
        return Long.MAX_VALUE;
      case SPACE_TOTAL:
        return Long.MAX_VALUE;
      default:
        throw new IOException("Unknown getSpace parameter: " + type);
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return false;
  }

  @Override
  public String[] list(String path) throws IOException {
    return null;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return null;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public void setConf(Object conf) {

  }

  public void setPermission(String path, String posixPerm) throws IOException {

  }

  /**
   * Internal function to delete a key in S3
   * @param key the key to delete
   * @return true if successful, false if an exception is thrown
   * @throws IOException
   */
  private boolean delete(String key) throws IOException {
    try {
      mClient.deleteObject(mBucket, key);
    } catch (ServiceException se) {
      LOG.error("Failed to delete " + key, se);
      return false;
    }
    return true;
  }

  private S3Object getObjectDetails(String key) throws IOException {
    try {
      return mClient.getObjectDetails(mBucket, key);
    } catch (ServiceException se) {
      LOG.error("File does not exist");
      handleException(se);
      return null;
    }
  }

  /**
   * Transforms JetS3t's Service Exceptions to IOExceptions
   * @throws IOException
   */
  private void handleException(ServiceException se) throws IOException {
    if (se.getCause() instanceof IOException) {
      throw (IOException) se.getCause();
    } else {
      // Unexpected Service Exception
    }
  }
}
