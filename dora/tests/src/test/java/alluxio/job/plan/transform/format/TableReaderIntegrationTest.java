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

package alluxio.job.plan.transform.format;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.job.JobIntegrationTest;
import alluxio.job.plan.transform.PartitionInfo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Unit tests for {@link TableReader}.
 */
public class TableReaderIntegrationTest extends JobIntegrationTest {
  @Rule
  public TemporaryFolder mTmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private PartitionInfo mPartitionInfo = new PartitionInfo("serde", "inputformat",
      new HashMap<>(), new HashMap<>(), new ArrayList<>());

  @Test
  public void createReaderWithoutScheme() throws Exception {
    AlluxioURI uri = new AlluxioURI("/CREATE_READER_WITHOUT_SCHEME");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_SCHEME.getMessage(uri));
    TableReader.create(uri, mPartitionInfo).close();
  }

  @Test
  public void createAlluxioReaderWithoutAuthority() throws Exception {
    AlluxioURI uri = new AlluxioURI("alluxio", null, "/CREATE_ALLUXIO_READER_WITHOUT_AUTHORITY");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_AUTHORITY.getMessage(uri));
    TableReader.create(uri, mPartitionInfo).close();
  }
}
