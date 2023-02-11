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

package alluxio.proxy.s3;

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is a wrapper for InputStream which limit rate when reading bytes.
 */
public class RateLimitInputStream extends InputStream {

  private final InputStream mInputStream;
  private final RateLimiter[] mRateLimiters;

  /**
   * Constructs a new {@link RateLimitInputStream}.
   *
   * @param inputStream  Original stream to be limited
   * @param rateLimiters RateLimiters to limit Maximal reading bytes per second
   */
  public RateLimitInputStream(InputStream inputStream, RateLimiter... rateLimiters) {
    mInputStream = inputStream;
    mRateLimiters = rateLimiters;
  }

  @Override
  public int read() throws IOException {
    acquire(1);
    return mInputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    acquire(Math.min(b.length - off, len));
    return mInputStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  private void acquire(int permits) {
    for (RateLimiter rateLimiter : mRateLimiters) {
      if (rateLimiter == null) {
        continue;
      }
      rateLimiter.acquire(permits);
    }
  }
}
