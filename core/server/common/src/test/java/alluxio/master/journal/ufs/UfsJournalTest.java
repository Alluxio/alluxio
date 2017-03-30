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

package alluxio.master.journal.ufs;

import alluxio.util.URIUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;

/**
 * Unit tests for {@link UfsJournal}.
 */
public final class UfsJournalTest {

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;

  @Before
  public void before() throws Exception {
    mJournal = new UfsJournal(URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster"));
  }

  @Test
  public void filename() throws Exception {
    String name = mJournal.encodeLogFileLocation(0x10, 0x100).toString();
    Assert.assertEquals(URIUtils.appendPathOrDie(mJournal.getLogDir(), "0x10-0x100"), name);
  }
}
