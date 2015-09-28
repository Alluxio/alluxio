/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import tachyon.TachyonURI;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.LineageInfo;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemMaster.class})
public final class LineageMasterTest {
  private LineageMaster mLineageMaster;
  private FileSystemMaster mFileSystemMaster;
  private Job mJob;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    Journal journal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mLineageMaster = new LineageMaster(mFileSystemMaster, journal);
    mLineageMaster.start(true);
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @Test
  public void listLineagesTest() throws Exception {
    mLineageMaster.createLineage(Lists.<TachyonURI>newArrayList(),
        Lists.newArrayList(new TachyonURI("/test1")), mJob);
    mLineageMaster.createLineage(Lists.newArrayList(new TachyonURI("/test1")),
        Lists.newArrayList(new TachyonURI("/test2")), mJob);
    List<LineageInfo> info = mLineageMaster.listLineages();
    Assert.assertEquals(2, info.size());
  }
}
