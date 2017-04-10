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

import alluxio.Constants;
import alluxio.clock.Clock;
import alluxio.clock.SystemClock;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalFactory;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link BlockMaster} instance.
 */
@ThreadSafe
public final class BlockMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterFactory.class);

  /**
   * Constructs a new {@link BlockMasterFactory}.
   */
  public BlockMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public BlockMaster create(MasterRegistry registry, JournalFactory journalFactory) {
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", BlockMaster.class.getName());
    return new DefaultBlockMaster(registry, journalFactory, new SystemClock(),
        ExecutorServiceFactories
            .fixedThreadPoolExecutorServiceFactory(Constants.BLOCK_MASTER_NAME, 2));
  }

  /**
   * Creates a new instance of {@link BlockMaster} with clock and executor service factory.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   * @return a new {@link BlockMaster} instance or null if the master is not enabled
   **/
  public BlockMaster create(MasterRegistry registry, JournalFactory journalFactory, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", BlockMaster.class.getName());
    return new DefaultBlockMaster(registry, journalFactory, clock, executorServiceFactory);
  }
}
