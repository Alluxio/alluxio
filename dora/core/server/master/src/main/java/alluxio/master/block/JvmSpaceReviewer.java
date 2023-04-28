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

package alluxio.master.block;

import alluxio.grpc.GetRegisterLeasePRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This reviews the heap allocation status to decide whether a request should be
 * accepted.
 */
public class JvmSpaceReviewer {
  private static final Logger LOG = LoggerFactory.getLogger(JvmSpaceReviewer.class);

  // It is observed in tests that if a RegisterWorkerPRequest contains 1 million blocks,
  // processing the request will take 200M-400M heap allocation.
  // Here we use the upper bound to do the estimation.
  public static final int BLOCK_COUNT_MULTIPLIER = 400;

  private final Runtime mRuntime;

  JvmSpaceReviewer(Runtime runtime) {
    mRuntime = runtime;
  }

  /**
   * Checks the current JVM usage to see if the request can be accepted without over-committing
   * the heap.
   */
  boolean reviewLeaseRequest(GetRegisterLeasePRequest request) {
    long blockCount = request.getBlockCount();
    long bytesAvailable = getAvailableBytes();
    long estimatedSpace = blockCount * BLOCK_COUNT_MULTIPLIER;
    if (bytesAvailable > estimatedSpace) {
      LOG.info("{} bytes available on master. The register request with {} blocks is estimated to"
          + " need {} bytes. ", bytesAvailable, blockCount, estimatedSpace);
      return true;
    } else {
      LOG.info("{} bytes available on master. The register request with {} blocks is estimated to"
              + " need {} bytes. Rejected the request.",
          bytesAvailable, blockCount, estimatedSpace);
      return false;
    }
  }

  /**
   * Calculates the available space in the heap based on the runtime.
   * Ref: <a href="https://stackoverflow.com/a/18375641/4933827">explanation</a>
   */
  private long getAvailableBytes() {
    return mRuntime.maxMemory() - (mRuntime.totalMemory() - mRuntime.freeMemory());
  }
}
