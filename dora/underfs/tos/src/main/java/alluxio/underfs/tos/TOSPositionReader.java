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

package alluxio.underfs.tos;

import alluxio.underfs.ObjectPositionReader;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.model.object.GetObjectV2Input;
import com.volcengine.tos.model.object.GetObjectV2Output;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link ObjectPositionReader} that reads from TOS object store.
 */
@ThreadSafe
public class TOSPositionReader extends ObjectPositionReader {

  /**
   * Client for operations with TOS.
   */
  protected final TOSV2 mClient;

  /**
   * @param client             the Tencent TOS client
   * @param bucketName         the bucket name
   * @param path               the file path
   * @param fileLength         the file length
   */
  public TOSPositionReader(TOSV2 client, String bucketName,
                           String path, long fileLength) {
    // TODO(lu) path needs to be transformed to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }

  @Override
  protected InputStream openObjectInputStream(
      long position, int bytesToRead) throws IOException {
    GetObjectV2Output output;
    try {
      ObjectMetaRequestOptions options = new ObjectMetaRequestOptions();
      options.setRange(position, position + bytesToRead - 1);
      GetObjectV2Input input = new GetObjectV2Input().setBucket(mBucketName)
          .setKey(mPath).setOptions(options);
      output = mClient.getObject(input);
    } catch (TosClientException e) {
      String errorMessage = String
          .format("TOS Client Exception: Failed to get object: %s bucket: %s", mPath, mBucketName);
      throw new IOException(errorMessage, e);
    } catch (TosServerException e) {
      String errorMessage = String
          .format("TOS Server Exception: Failed to get object: %s bucket: %s", mPath, mBucketName);
      throw new IOException(errorMessage, e);
    }
    return output.getContent();
  }
}
