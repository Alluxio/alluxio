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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link UfsJournal}.
 */
public class UfsJournalConfTest {
  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void emptyConfiguration() throws Exception {
    UnderFileSystemConfiguration conf = UfsJournal.getJournalUfsConf();
    Assert.assertTrue(conf.getMountSpecificConf().isEmpty());
  }

  @Test
  public void nonEmptyConfiguration() throws Exception {
    PropertyKey key =
        PropertyKey.Template.MASTER_JOURNAL_UFS_OPTION_PROPERTY
            .format(PropertyKey.UNDERFS_LISTING_LENGTH.toString());
    String value = "10000";
    ServerConfiguration.set(key, value);
    UnderFileSystemConfiguration conf = UfsJournal.getJournalUfsConf();
    Assert.assertEquals(value, conf.get(PropertyKey.UNDERFS_LISTING_LENGTH));
    Assert.assertEquals(1, conf.getMountSpecificConf().size());
  }
}
