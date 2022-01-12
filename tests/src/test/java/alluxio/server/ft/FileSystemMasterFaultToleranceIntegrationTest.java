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

package alluxio.server.ft;

import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.OperationId;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.UUID;

public final class FileSystemMasterFaultToleranceIntegrationTest extends BaseIntegrationTest {
  private static final int CLUSTER_WAIT_TIMEOUT_MS = 120 * Constants.SECOND_MS;
  private static final String TEST_USER = "test";

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster;

  @Rule
  public TestName mTestName = new TestName();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser =
      new AuthenticatedUserRule(TEST_USER, ServerConfiguration.global());

  @Before
  public final void before() throws Exception {
    mMultiMasterLocalAlluxioCluster = new MultiMasterLocalAlluxioCluster(2, 0);
    mMultiMasterLocalAlluxioCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "60sec");
    ServerConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "1sec");
    ServerConfiguration.set(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT, "30sec");
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "0sec");
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER);
    mMultiMasterLocalAlluxioCluster.start();
  }

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();
  }

  @Test
  public void partitionTolerantCreateFile() throws Exception {
    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");
    // Create context1 with unique operation id.
    CreateFileContext context = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    // Create context2 with unique operation id.
    CreateFileContext context2 = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));

    // Run partition tolerance test on leading master.
    {
      // Acquire file-system-master of leading master.
      final FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster
          .getLocalAlluxioMaster().getMasterProcess().getMaster(FileSystemMaster.class);

      // Create the path the first time.
      leadingFsMaster.createFile(testPath1, context);
      // Create the path the second time with the same context.
      // It should just return successfully.
      FileInfo fileInfo = leadingFsMaster.createFile(testPath1, context);
      Assert.assertEquals(fileInfo.getFileId(), leadingFsMaster.getFileId(testPath1));

      // Create the file again with a different context.
      // It should fail with `FileAlreadyExistException`.
      assertThrows(FileAlreadyExistsException.class,
          () -> leadingFsMaster.createFile(testPath1, context2));
    }

    // Promote standby to be a leader and reset test state.
    mMultiMasterLocalAlluxioCluster.stopLeader();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
    mAuthenticatedUser.resetUser();

    // Run partition tolerance test on the *new* leading master.
    {
      // Acquire file-system-master of leading master.
      final FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster
          .getLocalAlluxioMaster().getMasterProcess().getMaster(FileSystemMaster.class);

      // Creating on the new leader with the original operation-id should succeed.
      FileInfo fileInfo = leadingFsMaster.createFile(testPath1, context);
      Assert.assertEquals(fileInfo.getFileId(), leadingFsMaster.getFileId(testPath1));

      // Creating on the new leader with a different operation-id should fail.
      assertThrows(FileAlreadyExistsException.class,
          () -> leadingFsMaster.createFile(testPath1, context2));
    }
  }

  @Test
  public void partitionTolerantCompleteFile() throws Exception {
    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");
    // Create context1 with unique operation id.
    CompleteFileContext context = CompleteFileContext
        .mergeFrom(CompleteFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    // Create context2 with unique operation id.
    CompleteFileContext context2 = CompleteFileContext
        .mergeFrom(CompleteFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));

    // Run partition tolerance test on leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Create the path to complete.
      leadingFsMaster.createFile(testPath1, CreateFileContext.defaults());

      // Complete the path the first time.
      leadingFsMaster.completeFile(testPath1, context);
      // Complete the path the second time with the same context.
      // It should just return successfully.
      leadingFsMaster.completeFile(testPath1, context);

      // Complete the file again with a different context.
      // It should fail with `FileAlreadyCompletedException`.
      assertThrows(FileAlreadyCompletedException.class,
          () -> leadingFsMaster.completeFile(testPath1, context2));
    }

    // Promote standby to be a leader and reset test state.
    mMultiMasterLocalAlluxioCluster.stopLeader();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
    mAuthenticatedUser.resetUser();

    // Run partition tolerance test on the *new* leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Completing the file on the new leader with the original operation-id should succeed.
      leadingFsMaster.completeFile(testPath1, context);

      // Creating on the new leader with a different operation-id.
      // It should fail with `FileAlreadyCompletedException`.
      assertThrows(FileAlreadyCompletedException.class,
          () -> leadingFsMaster.completeFile(testPath1, context2));
    }
  }

  @Test
  public void partitionTolerantDeleteFile() throws Exception {
    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");
    // Create context1 with unique operation id.
    DeleteContext context = DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    // Create context2 with unique operation id.
    DeleteContext context2 = DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Create the path to delete.
      leadingFsMaster.createFile(testPath1, CreateFileContext.defaults());
      leadingFsMaster.completeFile(testPath1, CompleteFileContext.defaults());

      // Delete the path the first time.
      leadingFsMaster.delete(testPath1, context);
      // Delete the path the second time with the same context.
      // It should just return successfully.
      leadingFsMaster.delete(testPath1, context);

      // Delete the path again with a different context.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.delete(testPath1, context2));
    }

    // Promote standby to be a leader and reset test state.
    mMultiMasterLocalAlluxioCluster.stopLeader();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
    mAuthenticatedUser.resetUser();

    // Run partition tolerance test on the *new* leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);
      // Deleting the file on the new leader with the original operation-id should succeed.
      leadingFsMaster.delete(testPath1, context);

      // Deleting on the new leader with a different operation-id.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.delete(testPath1, context2));
    }
  }

  @Test
  public void partitionTolerantDeleteDirectory() throws Exception {
    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");
    // Create context1 with unique operation id.
    DeleteContext context = DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    // Create context2 with unique operation id.
    DeleteContext context2 = DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));

    // Run partition tolerance test on leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Create the path to delete.
      leadingFsMaster.createDirectory(testPath1, CreateDirectoryContext.defaults());

      // Delete the path the first time.
      leadingFsMaster.delete(testPath1, context);
      // Delete the path the second time with the same context.
      // It should just return successfully.
      leadingFsMaster.delete(testPath1, context);

      // Delete the path again with a different context.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.delete(testPath1, context2));
    }

    // Promote standby to be a leader and reset test state.
    mMultiMasterLocalAlluxioCluster.stopLeader();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
    mAuthenticatedUser.resetUser();

    // Run partition tolerance test on the *new* leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Deleting the path on the new leader with the original operation-id should succeed.
      leadingFsMaster.delete(testPath1, context);

      // Deleting on the new leader with a different operation-id.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.delete(testPath1, context2));
    }
  }

  @Test
  public void partitionTolerantRename() throws Exception {
    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");
    AlluxioURI testPath2 = new AlluxioURI("/testPath2");
    // Create context1 with unique operation id.
    RenameContext context = RenameContext
        .mergeFrom(RenamePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));
    // Create context2 with unique operation id.
    RenameContext context2 = RenameContext
        .mergeFrom(RenamePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setOperationId(new OperationId(UUID.randomUUID()).toFsProto())));

    // Run partition tolerance test on leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Create the path to rename.
      leadingFsMaster.createDirectory(testPath1, CreateDirectoryContext.defaults());

      // Rename the path the first time.
      leadingFsMaster.rename(testPath1, testPath2, context);
      // Renaming the path the second time with the same context.
      // It should just return successfully.
      leadingFsMaster.rename(testPath1, testPath2, context);

      // Rename the path again with a different context.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.rename(testPath1, testPath2, context2));
    }

    // Promote standby to be a leader and reset test state.
    mMultiMasterLocalAlluxioCluster.stopLeader();
    mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
    mAuthenticatedUser.resetUser();

    // Run partition tolerance test on the *new* leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystemMaster leadingFsMaster = mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster()
          .getMasterProcess().getMaster(FileSystemMaster.class);

      // Renaming the path on the new leader with the original operation-id should succeed.
      leadingFsMaster.rename(testPath1, testPath2, context);

      // Renaming on the new leader with a different operation-id.
      // It should fail with `FileDoesNotExistException`.
      assertThrows(FileDoesNotExistException.class,
          () -> leadingFsMaster.rename(testPath1, testPath2, context2));
    }
  }
}
