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

package alluxio.job.plan.transform.format.tables;

import alluxio.job.plan.transform.format.TableWriter;

/**
 * Determines whether to commit a table file based on the size of the file in bytes.
 * The last committed table file might have fewer bytes than the specified size.
 */
public class BytesCommitter implements Committer {
  private final long mBytes;

  /**
   * @param bytes the size of a table file in bytes
   */
  public BytesCommitter(long bytes) {
    mBytes = bytes;
  }

  @Override
  public boolean shouldCommit(TableWriter writer) {
    return writer.getBytes() >= mBytes;
  }
}
