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

package alluxio.underfs.gcs;

import alluxio.underfs.ObjectPositionReader;

import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;

import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link ObjectPositionReader} that reads from GCS object store.
 */
@ThreadSafe
public class GCSPositionReader extends ObjectPositionReader {

  /**
   * Client for operations with Aliyun OSS.
   */
  protected final GoogleStorageService mClient;

  /**
   * @param client     the Aliyun OSS client
   * @param bucketName the bucket name
   * @param path       the file path
   * @param fileLength the file length
   */
  public GCSPositionReader(GoogleStorageService client,
      String bucketName, String path, long fileLength) {
    // TODO(lu) path needs to be transformed to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }

  @Override
  protected InputStream openObjectInputStream(
      long position, int bytesToRead) {
    GSObject object;
    try {
      object = mClient.getObject(mBucketName, mPath, null,
          null, null, null, position, position + bytesToRead - 1);
    } catch (ServiceException e) {
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      throw new RuntimeException(errorMessage, e);
    }
    try {
      return object.getDataInputStream();
    } catch (ServiceException e) {
      String errorMessage = String
          .format("Failed to open GCS InputStream");
      throw new RuntimeException(errorMessage, e);
    }
  }
}
