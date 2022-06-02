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

import static alluxio.master.file.cmdmanager.command.CommandInfo.CmdOptions;

import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.wire.Status;
import alluxio.master.file.cmdmanager.command.CmdType;
import alluxio.master.file.cmdmanager.command.CommandInfo;
import alluxio.master.file.cmdmanager.command.ExecutionStatus;
import alluxio.master.file.cmdmanager.command.Task;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Filesystem Command manager which controls distributed job operations.
 */
public final class FileSystemCommandManager implements DelegatingJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemCommandManager.class);
  private final CmdEntry mCmdEntry = new CmdEntry();
  private final Map<Long, CommandStatus> mStatusMap = Maps.newHashMap();
  private final Scheduler mScheduler = new Scheduler(null);

  /**
   * Constructor.
   */
  public FileSystemCommandManager() {}

  /**
   * Validate the command information.
   * @param commandInfo command information
   * @return boolean value on whether the command is validated for scheduling or not
   */
  public boolean validate(CommandInfo commandInfo) {
    CmdOptions options = commandInfo.getCmdOptions();
    String path = commandInfo.getPath();
    ValidationStatus status = mCmdEntry.getValidationStatus(path, options);

    if (status.equals(ValidationStatus.New)) {
      return true;
    }

    if (status.equals(ValidationStatus.Update_Options)) {
      CommandInfo info = mCmdEntry.mCommandPathToInfo.get(path);
      long commandId = info.getId();
      /*Just update command status map with new options.*/
      CommandStatus commandStatus = mStatusMap.get(commandId);
      commandStatus.updateOptions(options);
    }
    return false;
  }

  /**
   * Schedule a command to run.
   * @param commandInfo command meta information
   * @throws ResourceExhaustedException throw ResourceExhaustedException
   * @throws InterruptedException throw InterruptedException
   */
  public void schedule(CommandInfo commandInfo)
          throws ResourceExhaustedException, InterruptedException {
    long commandId = commandInfo.getId();
    CommandStatus commandStatus = new CommandStatus(commandInfo.getId(),
            commandInfo.getPath(), commandInfo.getCmdOptions());
    mScheduler.schedule(commandStatus);
    mStatusMap.put(commandId, commandStatus);
  }

  /**
   * Get status for a command.
   * @param id command id
   * @return running status
   */
  public Status getStatus(Long id) {
    return null;
  }

  /**
   * Stop a command for running.
   * @param id command id
   */
  public void stop(Long id) {
  }

  /**
   * Close the command manager.
   * @throws IOException IOException
   */
  public void close() throws IOException {
  }

  @Override
  public Journaled getDelegate() {
    return mCmdEntry;
  }

  static class Scheduler {
    private static final int CAPACITY = 100;
    private static final long TIMEOUT = 100;
    private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<CommandStatus> mCommandQueue = new LinkedBlockingQueue<>(CAPACITY);
    private int mCurrentSize = 0;
    private final Client mClient;
    private long mTimeoutCounts;
    private long mRTCounts;

    public Scheduler(Client client) {
      if (client == null) {
        mClient = new Client();
      } else {
        mClient = client;
      }
      mTimeoutCounts = 0;
      mRTCounts = 0;
    }

    void schedule(CommandStatus commandStatus)
            throws ResourceExhaustedException, InterruptedException {
      if (mCurrentSize == CAPACITY) {
        throw new ResourceExhaustedException(
                "Insufficient capacity to enqueue tasks!");
      }

      boolean offered = mCommandQueue.offer(commandStatus, TIMEOUT, TimeUnit.MILLISECONDS);
      if (offered) {
        mCurrentSize++;
      } else {
        LOG.warn("Cannot enqueue commandStatus to the queue, may lose track on this command!"
                + commandStatus.getDetailedInfo());
      }

      mExecutorService.submit(() -> {
        try {
          mClient.runCommand(commandStatus);
        } catch (AlluxioRuntimeException e) {
          mRTCounts++;
          handleErrorOnStatuses(); // handle based on status
        } catch (TimeoutException e) {
          mTimeoutCounts++;
          // add retry and handle timeout caused by checking available workers
        }
      });
    }

    void start() {
      Thread thread = new Thread(() -> {
        while (!Thread.interrupted()) {
          if (!mCommandQueue.isEmpty()) {
            CommandStatus commandStatus = mCommandQueue.peek();
            if (commandStatus.getCommandStatus() == null
                    || commandStatus.getCommandStatus().isFinished()) {
              mCommandQueue.poll();
              mCurrentSize--;
            }
          }
        }
      });
      thread.start();
    }

    void handleErrorOnStatuses() {
    }

    long getTimeoutCounts() {
      return mTimeoutCounts;
    }

    long getRTCounts() {
      return mRTCounts;
    }
  }

  static class Client {
    private final AtomicLong mIdGenerator = new AtomicLong();

    void runCommand(CommandStatus commandStatus) throws AlluxioRuntimeException, TimeoutException {
      BlockIterator<Long> blockIterator = new BlockIterator<>(commandStatus.getPath());

      while (blockIterator.hasNextBatch()) {
        ExecutionWorkerInfo worker = getNextAvailableWorker(); // Get a worker to handle the task.
        if (worker == null) { // if no workers available, continue
          continue;
        }

        List<Long> blockIds = blockIterator.getNextBatchBlocks();
        Task task = new Task(blockIds, getNextTaskId());
        commandStatus.addTask(task);
        ExecutionStatus status = worker.execute(task);
        status.handleCompletion();
        task.setExecutionStatus(status);
      }
    }

    private long getNextTaskId() {
      return mIdGenerator.incrementAndGet();
    }

    private static ExecutionWorkerInfo getNextAvailableWorker() throws TimeoutException {
      // update currently available workers and get a next available worker.
      return null;
    }
  }

  static class CommandStatus {
    private final long mCommandId;
    private final String mPath;
    private List<Task> mTasks = Lists.newArrayList();
    private final CmdOptions mOptions;

    public CommandStatus(long commandId, String path, CmdOptions options) {
      mCommandId = commandId;
      mPath = path;
      mOptions = options;
    }

    public long getCommandId() {
      return mCommandId;
    }

    public Status getCommandStatus() {
      return null;
    }

    /*
     * Only update bandwidth.
     */
    public void updateOptions(CmdOptions options) {
      mOptions.getBandwidth().update(options.getBandwidth().getBandWidthValue());
    }

    public void addTask(Task t) {
      mTasks.add(t);
    }

    public String getPath() {
      return mPath;
    }

    public List<Task> getCommandTasks() {
      return mTasks;
    }

    public String getDetailedInfo() {
      return "";
    }
  }

  private enum ValidationStatus {
    New,
    Update_Options,
    Ignored
  }

  static class BlockIterator<T> {
    /**
     * Constructor to create a BlockIterator.
     * @param filePath file path
     */
    public BlockIterator(String filePath) {
    }

    public List<T> getNextBatchBlocks() throws AlluxioRuntimeException {
      return null;
    }

    /**
     * Whether the iterator has a next complete or partial batch.
     * @return boolean
     */
    public boolean hasNextBatch() {
      return true;
    }
  }

  private static class CmdEntry implements Journaled {
    private final Map<String, CommandInfo>
            mCommandPathToInfo = Maps.newHashMap();
    private final JobIdGenerator mJobIdGenerator = new JobIdGenerator();

    @Override
    public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(mCommandPathToInfo.entrySet().stream()
              .map(e -> Journal.JournalEntry.newBuilder().setCommandEntry(
                      File.CommandEntry.newBuilder()
                              .setType(e.getValue().getType().toProto())
                              .setSrcFilePath(e.getKey()))
                      .build())
                      .iterator());
    }

    @Override
    public boolean processJournalEntry(Journal.JournalEntry entry) {
      if (entry.hasCommandEntry()) {
        apply(entry.getCommandEntry());
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

    private void apply(File.CommandEntry entry) {
      long cmdId = mJobIdGenerator.getNewJobId();
      mCommandPathToInfo.put(entry.getSrcFilePath(),
              new CommandInfo(cmdId, entry.getSrcFilePath(),
                      CmdType.fromProto(entry.getType()),
                      entry.getOptions().getBandWidth()));
    }

    /**
     * Check command validation status, perform commandInfo option update accordingly.
     * @param path the file path
     * @param options the command options
     * @return validation status
     */
    public ValidationStatus getValidationStatus(String path, CmdOptions options) {
      if (mCommandPathToInfo.containsKey(path)) {
        boolean isNewOption = mCommandPathToInfo.get(path).getCmdOptions().equals(options);
        if (isNewOption) {
          CommandInfo info = mCommandPathToInfo.get(path);
          /*Only update bandwidth here.*/
          long newBandWidth = options.getBandwidth().getBandWidthValue();
          info.getCmdOptions().getBandwidth().update(newBandWidth);
          return ValidationStatus.Update_Options;
        }
        return ValidationStatus.Ignored;
      }
      return ValidationStatus.New;
    }
  }
}
