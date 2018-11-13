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

package alluxio.underfs.kodo;

import alluxio.underfs.MultiRangeObjectInputStream;

import com.qiniu.common.QiniuException;

import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading a file from Kodo. This input stream returns 0 when calling read with an
 * empty buffer.
 */
public class KodoInputStream extends MultiRangeObjectInputStream {

  /**
   * Key of the file in Kodo to read.
   */
  private final String mKey;

  /**
   * The Kodo client for Kodo operations.
   */
  private final KodoClient mKodoclent;

  /**
   * The size of the object in bytes.
   */
  private final long mContentLength;

  KodoInputStream(String key, KodoClient kodoClient, long position) throws QiniuException {
    mKey = key;
    mKodoclent = kodoClient;
    mPos = position;
    mContentLength = kodoClient.getFileInfo(key).fsize;
  }

  /**
   * Open a new stream reading a range. When endPos > content length, the returned stream should
   * read till the last valid byte of the input. The behaviour is undefined when (startPos < 0),
   * (startPos >= content length), or (endPos <= 0).
   *
   * @param startPos start position in bytes (inclusive)
   * @param endPos end position in bytes (exclusive)
   * @return a new {@link InputStream}
   */
  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    return mKodoclent.getObject(mKey, startPos, endPos, mContentLength);
  }
}

