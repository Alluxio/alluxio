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

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.security.authorization.Mode;
import alluxio.server.ft.journal.raft.EmbeddedJournalIntegrationTestBase;

import org.junit.Test;

import java.util.List;

/**
 * A test class to test the file system master fault toleration when MergeJournalContext is used.
 */
public class FileSystemMasterMergeJournalContextIntegrationTest
    extends EmbeddedJournalIntegrationTestBase {

  private static final int GET_MASTER_TIMEOUT_MS = 1 * Constants.MINUTE_MS;
  private static final int NUM_MASTERS = 3;
  private static final int NUM_WORKERS = 0;

  @Test
  public void testForceFlush() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_FAILOVER)
        .setClusterName("EmbeddedJournalFaultTolerance_failover")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

    // Create paths for the test.
    AlluxioURI testPath1 = new AlluxioURI("/testPath1");

    SetAttributePOptions setAttributeContext =
        SetAttributePOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0777).toProto()).build();
    int mergeJournalContextMaxEntries =
        Configuration.getInt(
            PropertyKey.MASTER_RECURSIVE_OPERATION_JOURNAL_FORCE_FLUSH_MAX_ENTRIES);

    // Run partition tolerance test on leading master.
    {
      // Acquire file-system-master of leading master.
      FileSystem fileSystem = mCluster.getFileSystemClient();

      fileSystem.createDirectory(testPath1, CreateDirectoryContext.defaults().getOptions().build());

      // Create files to change attributes.
      for (int i = 0; i < mergeJournalContextMaxEntries * 3; ++i) {
        AlluxioURI path = new AlluxioURI("/testPath1/" + i);
        fileSystem.createFile(path, CreateFileContext.defaults().getOptions().build()).close();
      }

      // Change the attribute recursively, this will trigger a InodeTree.getDescendants call.
      fileSystem.setAttribute(testPath1, setAttributeContext);

      List<URIStatus> fileInfo =
          fileSystem.listStatus(testPath1, ListStatusContext.defaults().getOptions()
          .build());
      assertEquals(mergeJournalContextMaxEntries * 3, fileInfo.size());
      fileInfo.forEach(
          (it) -> assertEquals(0777, it.getMode())
      );
    }

    mCluster.waitForAndKillPrimaryMaster(GET_MASTER_TIMEOUT_MS);
    mCluster.getPrimaryMasterIndex(GET_MASTER_TIMEOUT_MS);
    {
      // Acquire file-system-master of leading master.
      FileSystem fileSystem = mCluster.getFileSystemClient();

      List<URIStatus> fileInfo =
          fileSystem.listStatus(testPath1, ListStatusContext.defaults().getOptions()
          .build());
      assertEquals(mergeJournalContextMaxEntries * 3, fileInfo.size());
      fileInfo.forEach(
          (it) -> assertEquals(0777, it.getMode())
      );
    }
    mCluster.notifySuccess();
  }
}
