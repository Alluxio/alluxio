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

package alluxio.master.table.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import alluxio.client.job.JobMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobConfig;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterTestUtils;
import alluxio.master.PortRegistry;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.table.DefaultTableMaster;
import alluxio.master.table.Partition;
import alluxio.master.table.TableMaster;
import alluxio.master.table.TestDatabase;
import alluxio.master.table.TestUdbFactory;
import alluxio.table.common.Layout;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nullable;

/**
 * See the design of {@link alluxio.master.table.transform.TransformManager}.
 *
 * There are two components that might crash:
 * 1. the {@link TableMaster} running the manager;
 * 2. the job service.
 *
 * The test verifies that the manager works in the following cases:
 *
 * 1. transformation can be executed, table partitions can be updated automatically, and
 * transformation job history is kept correctly;
 * 2. if table master is restarted, and job service completes the transformation jobs during
 * restarting, after restarting, the manager still updates the partitions' locations;
 * 3. if job master crashes before finishing the transformation jobs,
 * or a transformation job fails, the transformation job status and reason of failure are kept.
 *
 * The test mocks the manager's {@link alluxio.client.job.JobMasterClient} without running a real
 * job service.
 *
 * The test manually controls the manager's heartbeat by {@link HeartbeatScheduler}.
 */
public class TransformManagerTest {
  private static final int NUM_TABLES = 3;
  private static final int NUM_PARTITIONS = 2;
  private static final String DB = TestDatabase.TEST_UDB_NAME;
  private static final String TABLE1 = TestDatabase.getTableName(0);
  private static final String TABLE2 = TestDatabase.getTableName(1);
  private static final String TABLE3 = TestDatabase.getTableName(2);
  private static final String EMPTY_DEFINITION = "";
  private static final String DEFINITION1 = "file.count.max=1";
  private static final String DEFINITION2 = "file.count.max=2";

  private JournalSystem mJournalSystem;
  private TableMaster mTableMaster;
  private JobMasterClient mMockJobMasterClient;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public ManuallyScheduleHeartbeat mManualScheduler =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TABLE_TRANSFORMATION_MONITOR);

  @Before
  public void before() throws Exception {
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT, PortRegistry.getFreePort());
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, "UFS");
    ServerConfiguration.set(PropertyKey.TABLE_TRANSFORM_MANAGER_JOB_HISTORY_RETENTION_TIME, "1h");

    mJournalSystem = JournalTestUtils.createJournalSystem(mTemporaryFolder);
    mJournalSystem.format();

    CoreMasterContext context = MasterTestUtils.testMasterContext(mJournalSystem);
    mMockJobMasterClient = Mockito.mock(JobMasterClient.class);
    mTableMaster = new DefaultTableMaster(context, mMockJobMasterClient);

    start();

    TestDatabase.genTable(NUM_TABLES, NUM_PARTITIONS, false);
    mTableMaster
        .attachDatabase(TestUdbFactory.TYPE, "connect", DB, DB, Collections.emptyMap(), false);
  }

  @After
  public void after() throws Exception {
    stop();
  }

  /**
   * There should only be at most one running job on a table.
   */
  @Test
  public void noConcurrentJobOnSameTable() throws Exception {
    long jobId = transform(TABLE1, DEFINITION1);

    // Try to run another transformation on the same table, since the previous transformation on
    // the table hasn't finished, this transformation should not be able to proceed.
    expectException(IOException.class,
        ExceptionMessage.TABLE_BEING_TRANSFORMED.getMessage(Long.toString(jobId), TABLE1, DB));
    transform(TABLE1, DEFINITION2);
  }

  /**
   * A job with the same definition as the last transformation on the table is a repeated job,
   * a repeated job should not be executed.
   */
  @Test
  public void noRepeatedJobOnSameTable() throws Exception {
    long jobId = transform(TABLE1, DEFINITION1);
    mockJobStatus(jobId, Status.COMPLETED, null);
    heartbeat();

    expectException(IOException.class,
        ExceptionMessage.TABLE_ALREADY_TRANSFORMED.getMessage(DB, TABLE1, DEFINITION1));
    transform(TABLE1, DEFINITION1);
  }

  /**
   * When getting job information for a non-existing transformation, exception is thrown.
   */
  @Test
  public void getInfoForNonExistingTransformJob() throws Exception {
    assertTrue(mTableMaster.getAllTransformJobInfo().isEmpty());
    long nonExistingJobId = -1;

    expectException(IOException.class,
        ExceptionMessage.TRANSFORM_JOB_DOES_NOT_EXIST.getMessage(nonExistingJobId));
    mTableMaster.getTransformJobInfo(nonExistingJobId);
  }

  /**
   * This verifies what kind of definition will be used when the default definition is used.
   */
  @Test
  public void defaultJob() throws Exception {
    assertTrue(mTableMaster.getAllTransformJobInfo().isEmpty());

    // Starts 1 job.
    long jobId1 = transform(TABLE1, EMPTY_DEFINITION);
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId1), TABLE1,
        DefaultTableMaster.DEFAULT_TRANSFORMATION,
        jobId1, Status.RUNNING, null);
    // Updates job status in heartbeat.
    mockJobStatus(jobId1, Status.COMPLETED, null);
    heartbeat();

    // Checks that the layout for job1 is the transformed layout.
    assertEquals(1, mTableMaster.getAllTransformJobInfo().size());
    TransformJobInfo job1Info = mTableMaster.getTransformJobInfo(jobId1);
    checkTransformJobInfo(job1Info, TABLE1, DefaultTableMaster.DEFAULT_TRANSFORMATION, jobId1,
        Status.COMPLETED, null);
  }

  /**
   * 1. when neither table master nor job master restarts, job status and partition locations are
   * updated correctly;
   * 2. when table master restarts, since information of the previous running jobs are
   * journaled, once the jobs finish, their status and partition locations are updated.
   * But the history of finished jobs (either succeeded or failed) is lost because the history is
   * not journaled.
   */
  @Test
  public void jobHistory() throws Exception {
    assertTrue(mTableMaster.getAllTransformJobInfo().isEmpty());

    // Starts 3 jobs.
    long jobId1 = transform(TABLE1, DEFINITION1);
    long jobId2 = transform(TABLE2, DEFINITION2);
    long jobId3 = transform(TABLE3, DEFINITION1);

    // Verifies that all jobs are running.
    assertEquals(3, mTableMaster.getAllTransformJobInfo().size());
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId1), TABLE1, DEFINITION1, jobId1,
        Status.RUNNING, null);
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId2), TABLE2, DEFINITION2, jobId2,
        Status.RUNNING, null);
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId3), TABLE3, DEFINITION1, jobId3,
        Status.RUNNING, null);

    // Updates job status in heartbeat.
    mockJobStatus(jobId1, Status.COMPLETED, null);
    mockJobStatus(jobId2, Status.FAILED, "error");
    mockJobStatus(jobId3, Status.RUNNING, null);
    heartbeat();

    // Verifies that job status has been updated by the heartbeat.
    assertEquals(3, mTableMaster.getAllTransformJobInfo().size());
    TransformJobInfo job1Info = mTableMaster.getTransformJobInfo(jobId1);
    checkTransformJobInfo(job1Info, TABLE1, DEFINITION1, jobId1,
        Status.COMPLETED, null);
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId2), TABLE2, DEFINITION2, jobId2,
        Status.FAILED, "error");
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId3), TABLE3, DEFINITION1, jobId3,
        Status.RUNNING, null);

    restart();

    // Checks that the layout for job1 is the transformed layout.
    checkLayout(job1Info, TABLE1);

    // Restarting table master will lose history for finished jobs,
    // but history for running jobs are journaled and replayed.
    assertEquals(1, mTableMaster.getAllTransformJobInfo().size());
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId3), TABLE3, DEFINITION1, jobId3,
        Status.RUNNING, null);

    // Now completes job 3.
    mockJobStatus(jobId3, Status.COMPLETED, null);
    heartbeat();
    assertEquals(1, mTableMaster.getAllTransformJobInfo().size());
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId3), TABLE3, DEFINITION1, jobId3,
        Status.COMPLETED, null);
  }

  /**
   * If the job master crashes, since job info is not journaled in job master, when it restarts,
   * the previous transformation jobs cannot be found, the manager will update the status of these
   * jobs as failed with descriptive error message.
   */
  @Test
  public void jobMasterRestart() throws Exception {
    long jobId = transform(TABLE1, DEFINITION1);
    Mockito.when(mMockJobMasterClient.getJobStatus(jobId)).thenThrow(new NotFoundException("none"));
    heartbeat();
    assertEquals(1, mTableMaster.getAllTransformJobInfo().size());
    checkTransformJobInfo(mTableMaster.getTransformJobInfo(jobId), TABLE1, DEFINITION1, jobId,
        Status.FAILED, ExceptionMessage.TRANSFORM_JOB_ID_NOT_FOUND_IN_JOB_SERVICE.getMessage(
            jobId, DB, TABLE1, "none"));
  }

  private void start() throws Exception {
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mTableMaster.start(true);
  }

  private void stop() throws Exception {
    mTableMaster.stop();
    mJournalSystem.stop();
  }

  private void restart() throws Exception {
    stop();
    start();
  }

  private long getRandomJobId() {
    Random random = new Random();
    return random.nextLong();
  }

  private long transform(String table, String definition) throws Exception {
    Mockito.when(mMockJobMasterClient.run(any(JobConfig.class))).thenReturn(getRandomJobId());
    return mTableMaster.transformTable(DB, table, definition);
  }

  private void expectException(Class<? extends Throwable> cls, String msg) {
    mException.expect(cls);
    mException.expectMessage(msg);
  }

  /**
   * Assert that the transform job information is expected.
   * If status is {@link Status#COMPLETED}, assert that the partitions' locations are updated.
   *
   * @param info the transform job info
   * @param table the expected table name
   * @param definition the expected transform definition
   * @param jobId the expected job ID
   * @param status the expected job status
   * @param error the expected job error
   */
  private void checkTransformJobInfo(TransformJobInfo info, String table, String definition,
      long jobId, Status status, @Nullable String error) throws Exception {
    assertEquals(DB, info.getDb());
    assertEquals(table, info.getTable());
    assertEquals(definition, info.getDefinition());
    assertEquals(jobId, info.getJobId());
    assertEquals(status, info.getJobStatus());
    if (error != null) {
      assertEquals(error, info.getJobErrorMessage());
    } else {
      assertEquals("", info.getJobErrorMessage());
    }
    if (status == Status.COMPLETED) {
      checkLayout(info, table);
    }
  }

  /**
   * Checks that the layouts of table partitions are the transformed layouts in info.
   *
   * @param info the job information
   * @param table the table
   */
  private void checkLayout(TransformJobInfo info, String table) throws IOException {
    for (Map.Entry<String, Layout> specLayouts : info.getTransformedLayouts().entrySet()) {
      String spec = specLayouts.getKey();
      Layout layout = specLayouts.getValue();
      Partition partition = mTableMaster.getTable(DB, table).getPartition(spec);
      assertTrue(partition.isTransformed(info.getDefinition()));
      assertEquals(layout, partition.getLayout());
    }
  }

  private void mockJobStatus(long jobId, Status status, @Nullable String error)
      throws Exception {
    // Mock job status.
    PlanInfo jobInfo = new PlanInfo(jobId, "test", status, 0, error);
    Mockito.when(mMockJobMasterClient.getJobStatus(jobId)).thenReturn(jobInfo);
  }

  private void heartbeat() throws Exception {
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TABLE_TRANSFORMATION_MONITOR);
  }
}
