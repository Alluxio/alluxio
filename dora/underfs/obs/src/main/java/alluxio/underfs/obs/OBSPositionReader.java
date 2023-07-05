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

package alluxio.underfs.obs;

import alluxio.underfs.ObjectPositionReader;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObsObject;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link ObjectPositionReader} that reads from OBS object store.
 */
public class OBSPositionReader extends ObjectPositionReader {
  /**
   * Client for operations with OBS.
   */
  protected final ObsClient mClient;

  /**
   * @param client     the OBS client
   * @param bucketName the bucket name
   * @param path       the file path
   * @param fileLength the file length
   */
  public OBSPositionReader(ObsClient client, String bucketName, String path, long fileLength) {
    // TODO(lu) path needs to be transformed to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }

  @Override
  protected InputStream openObjectInputStream(
      long position, int bytesToRead) throws IOException {
    ObsObject object;
    try {
      GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
      getObjectRequest.setRangeStart(position);
      getObjectRequest.setRangeEnd(position + bytesToRead - 1);
      object = mClient.getObject(getObjectRequest);
    } catch (ObsException e) {
      String errorMessage = String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName);
      throw new IOException(errorMessage, e);
    }
    return object.getObjectContent();
  }
}
