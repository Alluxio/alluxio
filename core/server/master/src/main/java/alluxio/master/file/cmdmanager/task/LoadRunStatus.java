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

package alluxio.master.file.cmdmanager.task;

import alluxio.master.file.cmdmanager.command.FsCmdConfig;

import java.util.Collection;
import java.util.List;

/**
 * Class for command status in breakdown of blocks.
 */
public class LoadRunStatus implements RunStatus<BlockTask> {
  private List<BlockTask> mBlockTasks;
  private FsCmdConfig mConfig;

  /**
   * Constructor.
   * @param config the command config
   * @param blockTasks the list of block tasks
   */
  public LoadRunStatus(FsCmdConfig config, List<BlockTask> blockTasks) {
    mConfig = config;
    mBlockTasks = blockTasks;
  }

  @Override
  public Collection<BlockTask> getSubTasks() {
    return mBlockTasks;
  }
}
