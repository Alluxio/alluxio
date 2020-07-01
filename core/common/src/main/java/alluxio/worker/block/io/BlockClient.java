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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides basic tracking and representation for block reader/writer clients.
 */
public abstract class BlockClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockClient.class);

  /** Used to keep unique client ids. */
  private static final AtomicInteger NEXT_CLIENT_ID = new AtomicInteger(0);

  /** Internal client id. */
  private final int mClientId;
  /** the client type. */
  private final Type mClientType;

  /**
   * Creates an abstract block reader/writer.
   *
   * @param clientType the block client type
   */
  public BlockClient(Type clientType) {
    mClientId = NEXT_CLIENT_ID.getAndIncrement();
    mClientType = clientType;
    LOG.debug("BlockClient created: {}.", this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Id", mClientId)
        .add("Type", mClientType.name())
        .add("Class", getClass().getName())
        .toString();
  }

  @Override
  public void close() throws IOException {
    LOG.debug("BlockClient closed: {}.", this);
  }

  /**
   * Block client type.
   */
  public enum Type {
    READER, WRITER
  }
}
