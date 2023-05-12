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

package alluxio.underfs.oss;

import alluxio.PositionReader;
import alluxio.file.ReadTargetBuffer;

import alluxio.underfs.ObjectPositionReader;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link ObjectPositionReader} that reads from OSS object store.
 */
@ThreadSafe
public class OSSPositionReader extends ObjectPositionReader {

  /**
   * Client for operations with Aliyun OSS.
   */
  protected OSS mClient;

  /**
   * @param client the Aliyun OSS client
   * @param bucketName the bucket name
   * @param path the file path
   * @param fileLength the file length
   */
  public OSSPositionReader(OSS client, String bucketName, String path, long fileLength) {
    // TODO(lu) path needs to be transformed to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }


  @Override
  protected int readInternalRequest(
      long position, ReadTargetBuffer buffer,
      int bytesToRead, String errorMessage) throws IOException{
    OSSObject object;
    try {
      GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
      getObjectRequest.setRange(position, position + bytesToRead - 1);
      object = mClient.getObject(getObjectRequest);
    } catch (OSSException e) {
      throw new IOException(errorMessage, e);
    }

    // Range check approach: set range (inclusive start, inclusive end)
    // start: should be < file length, error out otherwise
    //        e.g. error out when start == 0 && fileLength == 0
    //        start < 0, read all
    // end: if start > end, read all
    //      if start <= end < file length, read from start to end
    //      if end >= file length, read from start to file length - 1

    int totalRead = 0;
    int currentRead = 0;

    try(InputStream in = object.getObjectContent()) {
      while (totalRead < bytesToRead) {
        currentRead = buffer.readFromInputStream(in, bytesToRead - totalRead);
        if (currentRead < 0) {
          break;
        }
        totalRead += currentRead;
      }
    }
    return totalRead == 0 ? currentRead : totalRead;
  }

}
