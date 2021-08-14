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

package alluxio.master.file.replication;

import static org.mockito.Mockito.mock;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.job.plan.replicate.ReplicationHandler;
import alluxio.job.wire.Status;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.CreatePathContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.proto.meta.Block;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unit tests for {@link ReplicationChecker}.
 */
public final class ReplicationCheckerTest {
  private static final String TEST_OWNER = "user1";
  private static final String TEST_GROUP = "";
  private static final Mode TEST_MODE = new Mode((short) 0755);
  private static final AlluxioURI TEST_FILE_1 = new AlluxioURI("/test1");
  private static final AlluxioURI TEST_FILE_2 = new AlluxioURI("/test2");
  private static final AlluxioURI TEST_FILE_3 = new AlluxioURI("/test3");
  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final List<Metric> NO_METRICS = ImmutableList.of();
  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION
      = ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final Map EMPTY = ImmutableMap.of();

  /**
   * A mock class of AdjustReplicationHandler, used to test the output of ReplicationChecker.
   */
  @ThreadSafe
  private static class MockHandler implements ReplicationHandler {
    private final Map<Long, Integer> mEvictRequests = Maps.newHashMap();
    private final Map<Long, Integer> mReplicateRequests = Maps.newHashMap();
    private final Map<Long, Pair<String, String>>
        mMigrateRequests = Maps.newHashMap();
    private final List<Long> mJobStatusRequests = Lists.newArrayList();
    private final Map<Long, Status> mJobStatus = Maps.newHashMap();

    private long mNextJobId = 0;

    public void setJobStatus(long jobId, Status status) {
      mJobStatus.put(jobId, status);
    }

    @Override
    public Status getJobStatus(long jobId) throws IOException {
      mJobStatusRequests.add(jobId);
      if (mJobStatus.containsKey(jobId)) {
        return mJobStatus.get(jobId);
      }
      return Status.RUNNING;
    }

    @Override
    public List<Long> findJobs(String jobName, Set<Status> statusList) throws IOException {
      return mJobStatus.entrySet().stream()
          .filter(x -> statusList.isEmpty() || statusList.contains(x.getValue()))
          .map(x -> x.getKey()).collect(Collectors.toList());
    }

    @Override
    public long evict(AlluxioURI uri, long blockId, int numReplicas) {
      mEvictRequests.put(blockId, numReplicas);
      return ++mNextJobId;
    }

    @Override
    public long replicate(AlluxioURI uri, long blockId, int numReplicas) {
      mReplicateRequests.put(blockId, numReplicas);
      return ++mNextJobId;
    }

    @Override
    public long migrate(AlluxioURI uri, long blockId, String workerHost, String mediumType) {
      mMigrateRequests.put(blockId, new Pair<>(workerHost, mediumType));
      return ++mNextJobId;
    }

    public Map<Long, Integer> getEvictRequests() {
      return mEvictRequests;
    }

    public Map<Long, Integer> getReplicateRequests() {
      return mReplicateRequests;
    }

    public Map<Long, Pair<String, String>> getMigrateRequests() {
      return mMigrateRequests;
    }
  }

  private CoreMasterContext mContext;
  private InodeStore mInodeStore;
  private InodeTree mInodeTree;
  private BlockMaster mBlockMaster;
  private ReplicationChecker mReplicationChecker;
  private MockHandler mMockReplicationHandler;
  private CreateFileContext mFileContext =
      CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
          .setMode(TEST_MODE.toProto())).setOwner(TEST_OWNER).setGroup(TEST_GROUP);
  private Set<Long> mKnownWorkers = Sets.newHashSet();

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, "UFS");
    MasterRegistry registry = new MasterRegistry();
    JournalSystem journalSystem = JournalTestUtils.createJournalSystem(mTestFolder);
    mContext = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(registry, mContext);
    mBlockMaster = new BlockMasterFactory().create(registry, mContext);
    InodeDirectoryIdGenerator directoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    UfsManager manager = mock(UfsManager.class);
    MountTable mountTable = new MountTable(manager, mock(MountInfo.class));
    InodeLockManager lockManager = new InodeLockManager();
    mInodeStore = mContext.getInodeStoreFactory().apply(lockManager);
    mInodeTree =
        new InodeTree(mInodeStore, mBlockMaster, directoryIdGenerator, mountTable, lockManager);

    journalSystem.start();
    journalSystem.gainPrimacy();
    mBlockMaster.start(true);

    ServerConfiguration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    ServerConfiguration
        .set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "test-supergroup");
    mInodeTree.initializeRoot(TEST_OWNER, TEST_GROUP, TEST_MODE, NoopJournalContext.INSTANCE);

    mMockReplicationHandler = new MockHandler();
    mReplicationChecker = new ReplicationChecker(mInodeTree, mBlockMaster,
        mContext.getSafeModeManager(), mMockReplicationHandler);
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  /**
   * Helper to create a file with a single block.
   *
   * @param path Alluxio path of the file
   * @param context context to create the file
   * @param pinLocation
   * @return the block ID
   */
  private long createBlockHelper(AlluxioURI path, CreatePathContext<?, ?> context,
      String pinLocation) throws Exception {
    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, LockPattern.WRITE_EDGE)) {
      List<Inode> created = mInodeTree.createPath(RpcContext.NOOP, inodePath, context);
      if (!pinLocation.equals("")) {
        mInodeTree.setPinned(RpcContext.NOOP, inodePath, true, ImmutableList.of(pinLocation), 0);
      }
      MutableInodeFile inodeFile = mInodeStore.getMutable(created.get(0).getId()).get().asFile();
      inodeFile.setBlockSizeBytes(1);
      inodeFile.setBlockIds(Arrays.asList(inodeFile.getNewBlockId()));
      inodeFile.setCompleted(true);
      mInodeStore.writeInode(inodeFile);
      return inodeFile.getBlockIdByIndex(0);
    }
  }

  /**
   * Helper to create and add a given number of locations for block ID.
   *
   * @param blockId ID of the block to add location
   * @param numLocations number of locations to add
   */
  private void addBlockLocationHelper(long blockId, int numLocations) throws Exception {
    // Commit blockId to the first worker.
    mBlockMaster.commitBlock(createWorkerHelper(0), 50L,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, 20L);

    // Send a heartbeat from other workers saying that it's added blockId.
    for (int i = 1; i < numLocations; i++) {
      heartbeatToAddLocationHelper(blockId, createWorkerHelper(i));
    }
  }

  /**
   * Helper to register a new worker.
   *
   * @param workerIndex the index of the worker in all workers
   * @return the created worker ID
   */
  private long createWorkerHelper(int workerIndex) throws Exception {
    WorkerNetAddress address = new WorkerNetAddress().setHost("host" + workerIndex).setRpcPort(1000)
        .setDataPort(2000).setWebPort(3000);
    long workerId = mBlockMaster.getWorkerId(address);
    if (!mKnownWorkers.contains(workerId)) {
      // Do not re-register works, otherwise added block will be removed
      mBlockMaster.workerRegister(workerId, ImmutableList.of(Constants.MEDIUM_MEM),
          ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
          ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
          RegisterWorkerPOptions.getDefaultInstance());
      mKnownWorkers.add(workerId);
    }
    return workerId;
  }

  /**
   * Helper to heartbeat to a worker and report a newly added block.
   *
   * @param blockId ID of the block to add location
   * @param workerId ID of the worker to heartbeat
   */
  private void heartbeatToAddLocationHelper(long blockId, long workerId) throws Exception {
    List<Long> addedBlocks = ImmutableList.of(blockId);
    Block.BlockLocation blockLocation =
        Block.BlockLocation.newBuilder().setWorkerId(workerId)
            .setTier(Constants.MEDIUM_MEM).setMediumType(Constants.MEDIUM_MEM).build();

    mBlockMaster.workerHeartbeat(workerId, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS,
        ImmutableMap.of(blockLocation, addedBlocks), NO_LOST_STORAGE, NO_METRICS);
  }

  @Test
  public void heartbeatWhenTreeIsEmpty() throws Exception {
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileWithinRange() throws Exception {
    mFileContext.getOptions().setReplicationMin(1).setReplicationMax(3);
    long blockId =
        createBlockHelper(TEST_FILE_1, mFileContext, "");
    // One replica, meeting replication min
    addBlockLocationHelper(blockId, 1);
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());

    // Two replicas, good
    heartbeatToAddLocationHelper(blockId, createWorkerHelper(1));
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());

    // Three replicas, meeting replication max, still good
    heartbeatToAddLocationHelper(blockId, createWorkerHelper(2));
    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileUnderReplicatedBy1() throws Exception {
    mFileContext.getOptions().setReplicationMin(1);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, "");

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 1);
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(expected, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileNeedsMove() throws Exception {
    mFileContext.getOptions().setReplicationMin(1);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, Constants.MEDIUM_SSD);
    addBlockLocationHelper(blockId, 1);

    mReplicationChecker.heartbeat();
    Map<Long, Pair<String, String>> expected =
        ImmutableMap.of(blockId, new Pair<>("host0", Constants.MEDIUM_SSD));
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
    Assert.assertEquals(expected, mMockReplicationHandler.getMigrateRequests());
  }

  @Test
  public void heartbeatFileDoesnotNeedMove() throws Exception {
    mFileContext.getOptions().setReplicationMin(1);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, Constants.MEDIUM_MEM);
    addBlockLocationHelper(blockId, 1);

    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getMigrateRequests());
  }

  @Test
  public void heartbeatFileUnderReplicatedBy10() throws Exception {
    mFileContext.getOptions().setReplicationMin(10);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, "");

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 10);
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(expected, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatMultipleFilesUnderReplicated() throws Exception {
    mFileContext.getOptions().setReplicationMin(1);
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileContext, "");
    mFileContext.getOptions().setReplicationMin(2);
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileContext, "");

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId1, 1, blockId2, 2);
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(expected, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileUnderReplicatedAndLost() throws Exception {
    mFileContext.getOptions().setReplicationMin(2);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, "");

    // Create a worker.
    long workerId = mBlockMaster.getWorkerId(new WorkerNetAddress().setHost("localhost")
        .setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(workerId, Collections.singletonList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE, RegisterWorkerPOptions.getDefaultInstance());
    mBlockMaster.commitBlock(workerId, 50L,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, 20L);

    // Indicate that blockId is removed on the worker.
    mBlockMaster.workerHeartbeat(workerId, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
        ImmutableList.of(blockId), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, NO_METRICS);

    mReplicationChecker.heartbeat();
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileOverReplicatedBy1() throws Exception {
    mFileContext.getOptions().setReplicationMax(1);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, "");
    addBlockLocationHelper(blockId, 2);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 1);
    Assert.assertEquals(expected, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFileOverReplicatedBy10() throws Exception {
    mFileContext.getOptions().setReplicationMax(1);
    long blockId = createBlockHelper(TEST_FILE_1, mFileContext, "");
    addBlockLocationHelper(blockId, 11);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId, 10);
    Assert.assertEquals(expected, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatMultipleFilesOverReplicated() throws Exception {
    mFileContext.getOptions().setReplicationMax(1);
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileContext, "");
    mFileContext.getOptions().setReplicationMax(2);
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileContext, "");
    addBlockLocationHelper(blockId1, 2);
    addBlockLocationHelper(blockId2, 4);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected = ImmutableMap.of(blockId1, 1, blockId2, 2);
    Assert.assertEquals(expected, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(EMPTY, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatFilesUnderAndOverReplicated() throws Exception {
    mFileContext.getOptions().setReplicationMin(2).setReplicationMax(-1);
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileContext, "");
    mFileContext.getOptions().setReplicationMin(0).setReplicationMax(3);
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileContext, "");
    addBlockLocationHelper(blockId1, 1);
    addBlockLocationHelper(blockId2, 5);

    mReplicationChecker.heartbeat();
    Map<Long, Integer> expected1 = ImmutableMap.of(blockId1, 1);
    Map<Long, Integer> expected2 = ImmutableMap.of(blockId2, 2);

    Assert.assertEquals(expected2, mMockReplicationHandler.getEvictRequests());
    Assert.assertEquals(expected1, mMockReplicationHandler.getReplicateRequests());
  }

  @Test
  public void heartbeatPartial() throws Exception {
    ServerConfiguration.set(PropertyKey.JOB_MASTER_JOB_CAPACITY, 20);
    mReplicationChecker = new ReplicationChecker(mInodeTree, mBlockMaster,
        mContext.getSafeModeManager(), mMockReplicationHandler);
    mFileContext.getOptions().setReplicationMin(3).setReplicationMax(-1);
    long blockId1 = createBlockHelper(TEST_FILE_1, mFileContext, "");
    long blockId2 = createBlockHelper(TEST_FILE_2, mFileContext, "");
    long blockId3 = createBlockHelper(TEST_FILE_3, mFileContext, "");
    addBlockLocationHelper(blockId1, 1);
    addBlockLocationHelper(blockId2, 1);
    addBlockLocationHelper(blockId3, 1);

    mReplicationChecker.heartbeat();
    final Map<Long, Integer> replicateRequests = mMockReplicationHandler.getReplicateRequests();
    Assert.assertEquals(2, replicateRequests.size());
    Assert.assertEquals(2, replicateRequests.values().toArray()[0]);
    Assert.assertEquals(2, replicateRequests.values().toArray()[1]);
    replicateRequests.clear();

    mMockReplicationHandler.setJobStatus(1, Status.RUNNING);
    mMockReplicationHandler.setJobStatus(2, Status.RUNNING);
    mReplicationChecker.heartbeat();
    Assert.assertEquals(0, replicateRequests.size());

    mMockReplicationHandler.setJobStatus(1, Status.FAILED);
    mReplicationChecker.heartbeat();
    Assert.assertEquals(1, replicateRequests.size());
    Assert.assertEquals(2, replicateRequests.values().toArray()[0]);

    replicateRequests.clear();
    addBlockLocationHelper(blockId1, 3);
    addBlockLocationHelper(blockId2, 3);
    mMockReplicationHandler.setJobStatus(2, Status.COMPLETED);
    mMockReplicationHandler.setJobStatus(3, Status.COMPLETED);

    mReplicationChecker.heartbeat();
    Assert.assertEquals(1, replicateRequests.size());
    Assert.assertTrue(replicateRequests.containsKey(blockId3));
    Assert.assertEquals(2, replicateRequests.values().toArray()[0]);
  }
}
