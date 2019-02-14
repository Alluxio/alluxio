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

package alluxio.master.file;

/**
 * Implementation of {@link DefaultBlockDeletionContext} which does nothing.
 */
public final class NoopBlockDeletionContext implements BlockDeletionContext {
  public static final NoopBlockDeletionContext INSTANCE = new NoopBlockDeletionContext();

  @Override
  public void registerBlockForDeletion(long blockId) {
    return;
  }

  @Override
  public void close() {
    return;
  }
}
