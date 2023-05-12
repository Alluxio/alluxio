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

package alluxio.worker.netty;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * A packet read task represents a task for reading packet according to request from Netty Client.
 * @param <T> type of read request {@link ReadRequestContext}
 */
public class PacketReadTask<T extends ReadRequestContext<?>> implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(PacketReadTask.class);

  private final String mTaskId;

  private final PacketReadTaskStateMachine<?> mStateMachine;

  /**
   * Creates an instance of the {@link PacketReadTask}.
   *
   * @param taskId the identifier for the packet read task
   * @param context context of the request to complete
   * @param channel the channel
   * @param packetReader the packet reader for reading packet
   */
  public PacketReadTask(String taskId, T context, Channel channel,
                        AbstractReadHandler<T>.PacketReader packetReader) {
    mTaskId = taskId;
    mStateMachine = new PacketReadTaskStateMachine<>(context, channel, packetReader);
  }

  @Override
  public Boolean call() {
    try {
      runInternal();
      return true;
    } catch (RuntimeException e) {
      LOG.error("Failed to run PacketReader.", e);
      throw e;
    }
  }

  private void runInternal() {
    mStateMachine.run();
  }

  /**
   * Cancel the packet read task.
   */
  public void cancelTask() {
    mStateMachine.cancel();
  }

  /**
   * Notify the task that there is an exception.
   * @param e
   */
  public void notifyChannelException(Error e) {
    mStateMachine.occurChannelException(e);
  }

  /**
   * Get the task ID.
   * @return taskId
   */
  public String getTaskId() {
    return mTaskId;
  }
}
