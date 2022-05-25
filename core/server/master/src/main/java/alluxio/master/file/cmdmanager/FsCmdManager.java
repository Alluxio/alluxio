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

package alluxio.master.file.cmdmanager;

import alluxio.client.block.BlockStoreClient;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.wire.Status;
import alluxio.master.file.cmdmanager.task.ExecutionWorkerInfo;
import alluxio.master.file.cmdmanager.task.BlockTask;
import alluxio.master.file.cmdmanager.task.RunStatus;
import alluxio.master.file.cmdmanager.command.Bandwidth;
import alluxio.master.file.cmdmanager.command.CmdType;
import alluxio.master.file.cmdmanager.command.FsCmdConfig;
import alluxio.master.file.cmdmanager.scheduler.CommandScheduler;
import alluxio.master.file.cmdmanager.task.DefaultWorkerAssigner;
import alluxio.master.file.cmdmanager.task.WorkerAssigner;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.retry.RetryPolicy;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The Fs Cmd manager which controls distributed job operations.
 */
public final class FsCmdManager implements AbstractFsCmdManager<FsCmdConfig, Long>,
        DelegatingJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(FsCmdManager.class);
  private final CommandScheduler<BlockTask> mScheduler;
  private final CmdEntry mCmdEntry;
  private final Map<Long, RunStatus> mRunningStatus = Maps.newHashMap();
  private final BlockStoreClient mClient; // todo: use correct clients to call worker api

  /**
   * Constructor.
   * @param scheduler the scheduler for FsCmdManager
   */
  public FsCmdManager(CommandScheduler<BlockTask> scheduler) {
    mScheduler = scheduler;
    mCmdEntry = new CmdEntry();
    mClient = null;
  }

  @Override
  public boolean validate(FsCmdConfig config) {
    CmdOptions options = new CmdOptions(config.getBandWidth());
    return mCmdEntry.isDuplicate(config.getSrcFilePath(), options);
  }

  /**
   * Generate blocks for a given FsCmdConfig. Schedule a distributed command to run.
   * @param config the command config
   * @param retryPolicy the retry policy
   */
  @Override
  public void schedule(FsCmdConfig config, RetryPolicy retryPolicy) {
    try {
      CommandInfo info = mCmdEntry.getCommandInfo(config.getSrcFilePath());
      long id = info.getId();

      WorkerAssigner<RunStatus<BlockTask>> assigner = new DefaultWorkerAssigner(mClient);
      Set<ExecutionWorkerInfo> workers = assigner.getWorkers();
      RunStatus<BlockTask> runStatus = assigner.createCommandAssignment(config, workers);
      Collection<BlockTask> tasks = runStatus.getSubTasks();
      mRunningStatus.put(id, runStatus); // Register command id and run-status to the map.
      tasks.forEach(t ->  {
        while (retryPolicy.attempt()) {
          try {
            mScheduler.schedule(t);
          } catch (ResourceExhaustedException e) {
            LOG.warn("Resource exhausted for task scheduling " + t.getName());
            // todo: will let user handle this case
            break;
          } catch (InterruptedException e) {
            LOG.warn("Interrupted for task scheduling " + t.getName()
                    + ", will reschedule with attempt " + retryPolicy.getAttemptCount());
          } catch (Exception e) {
            LOG.warn("Caught other exceptions for task scheduling " + t.getName()
                    + ", will reschedule with attempt " + retryPolicy.getAttemptCount()
                    + ", exception is " + e.getMessage());
          }
        }
      });
    } catch (JobDoesNotExistException e) {
      LOG.warn(String.format("No job exists for path %s", config.getSrcFilePath()));
    }
  }

  /**
   * Get status for a command.
   * @param id command id
   * @return running status
   */
  @Override
  public Status getStatus(Long id) {
    return null;
  }

  @Override
  public void update(FsCmdConfig config) {
  }

  /**
   * Stop a command for running.
   * @param id command id
   */
  @Override
  public void stop(Long id) {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Journaled getDelegate() {
    return mCmdEntry;
  }

  private static class CmdOptions {
    private Bandwidth mBandwidth;

    public CmdOptions(Bandwidth bandwidth) {
      mBandwidth = bandwidth;
    }

    public Bandwidth getBandwidth() {
      return mBandwidth;
    }

    public void setBandwidth(Bandwidth bandwidth) {
      mBandwidth = bandwidth;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CmdOptions that = (CmdOptions) o;
      return Objects.equal(mBandwidth, that.mBandwidth);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mBandwidth);
    }
  }

  /**
   * Class for command information.
   */
  public static class CommandInfo {
    private long mId; //Command Id.
    private CmdType mType;
    private CmdOptions mOptions;

    /**
     * Constructor.
     * @param id command id
     * @param type command type
     * @param bandwidth bandwidth
     */
    public CommandInfo(long id, CmdType type, long bandwidth) {
      mId = id;
      mType = type;
      mOptions = new CmdOptions(new Bandwidth(bandwidth));
    }

    /**
     * Get command Id.
     * @return the id
     */
    public long getId() {
      return mId;
    }

    /**
     * Get command type.
     * @return command type
     */
    public CmdType getType() {
      return mType;
    }

    /**
     * Get command options.
     * @return command options
     */
    public CmdOptions getCmdOptions() {
      return mOptions;
    }
  }

  /**
   * Class for file name meta info.
   */
  public static class FileNameInfo {
    private String mFileName;
    private long mFileSize;

    /**
     * Constructor.
     * @param fileName file name
     * @param size file size
     */
    public FileNameInfo(String fileName, long size) {
      mFileName = fileName;
      mFileSize = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FileNameInfo fileName = (FileNameInfo) o;
      return Objects.equal(mFileName, fileName.mFileName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mFileName, mFileSize);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileName", mFileName)
          .add("fileSize", mFileSize)
          .toString();
    }
  }

  private static class CmdEntry implements Journaled {
    private final Map<String, CommandInfo>
            mCommandPathToInfo = Maps.newHashMap();
    private final JobIdGenerator mJobIdGenerator = new JobIdGenerator();

    @Override
    public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(mCommandPathToInfo.entrySet().stream()
              .map(e -> Journal.JournalEntry.newBuilder().setUpdateCmdEntry(
                      File.UpdateCmdEntry.newBuilder()
                              .setType(e.getValue().getType().toProto())
                              .setSrcFilePath(e.getKey()))
                      // skip setting destFilePath for distLoad case
                      .build())
                      .iterator());
    }

    @Override
    public boolean processJournalEntry(Journal.JournalEntry entry) {
      if (entry.hasUpdateCmdEntry()) {
        apply(entry.getUpdateCmdEntry());
      } else {
        return false;
      }
      return true;
    }

    @Override
    public void resetState() {
      mCommandPathToInfo.clear();
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.CMD_ENTRY;
    }

    private void apply(File.UpdateCmdEntry entry) {
      long cmdId = mJobIdGenerator.getNewJobId();
      mCommandPathToInfo.put(entry.getSrcFilePath(),
              new CommandInfo(cmdId, CmdType.fromProto(entry.getType()),
                      entry.getBandWidth()));
    }

    /**
     * Whether the command is duplicate or not.
     * @param uri the file path
     * @param options the command options
     * @return boolean flag
     */
    public boolean isDuplicate(String uri, CmdOptions options) {
      if (mCommandPathToInfo.containsKey(uri)) {
        return mCommandPathToInfo.get(uri).getCmdOptions().equals(options);
      }
      return false;
    }

    public CommandInfo getCommandInfo(String uri) throws JobDoesNotExistException {
      if (!mCommandPathToInfo.containsKey(uri)) {
        throw new JobDoesNotExistException(
                String.format("Command for path %s does not exist", uri));
      }
      return mCommandPathToInfo.get(uri);
    }
  }
}
