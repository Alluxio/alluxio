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

package alluxio.master.journal.tool;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.file.Paths;

/**
 * Tests for {@link JournalToolTest}.
 */
@PrepareForTest({JournalTool.class})
@RunWith(PowerMockRunner.class)
public class JournalToolTest {

  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    PowerMockito.spy(JournalTool.class);
    PowerMockito.doNothing().when(JournalTool.class, "dumpJournal");
  }

  @Test
  public void defaultJournalDir() throws Throwable {
    JournalTool.main(new String[0]);
    String inputUri = Whitebox.getInternalState(JournalTool.class, "sInputDir");
    Assert.assertEquals(ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_FOLDER), inputUri);
  }

  @Test
  public void hdfsJournalDir() throws Throwable {
    String journalPath = "hdfs://namenode:port/alluxio/journal";
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FOLDER, journalPath);

    JournalTool.main(new String[0]);
    String inputUri = Whitebox.getInternalState(JournalTool.class, "sInputDir");
    Assert.assertEquals(journalPath, inputUri);
  }

  @Test
  public void absoluteLocalJournalInput() throws Throwable {
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FOLDER,
        "hdfs://namenode:port/alluxio/journal");

    String journalPath = "/path/to/local/file";
    JournalTool.main(new String[]{"-inputDir", journalPath});
    String inputUri = Whitebox.getInternalState(JournalTool.class, "sInputDir");
    Assert.assertEquals(journalPath, inputUri);
  }

  @Test
  public void relativeLocalJournalInput() throws Throwable {
    String journalPath = "fileA";
    JournalTool.main(new String[]{"-inputDir", journalPath});
    String inputUri = Whitebox.getInternalState(JournalTool.class, "sInputDir");
    Assert.assertEquals(
        Paths.get(System.getProperty("user.dir"), journalPath).toString(), inputUri);
  }
}
