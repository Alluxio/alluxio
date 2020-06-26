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

package alluxio.worker.block.io;

import com.google.common.base.MoreObjects;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides basic tracking and representation for block reader/writer clients.
 */
public abstract class AbstractBlockClient implements BlockClient {
  /** Used to keep unique client ids. */
  private static AtomicInteger sNextClientId = new AtomicInteger(0);

  /** Internal client id. */
  private int mClientId;
  /** the client type. */
  private Type mClientType;

  /**
   * Creates an abstract block reader/writer.
   *
   * @param clientType the block client type
   */
  public AbstractBlockClient(Type clientType) {
    mClientId = sNextClientId.getAndIncrement();
    mClientType = clientType;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Id", mClientId)
        .add("Type", mClientType.name())
        .toString();
  }

  /**
   * Block client type.
   */
  enum Type {
    READER, WRITER
  }
}
