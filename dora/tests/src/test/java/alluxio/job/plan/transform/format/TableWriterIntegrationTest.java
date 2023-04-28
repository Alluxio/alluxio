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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.job.JobIntegrationTest;
import alluxio.job.plan.transform.format.parquet.ParquetSchema;
import alluxio.job.plan.transform.format.parquet.ParquetTestUtils;
import alluxio.uri.Authority;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Unit tests for {@link TableWriter}.
 */
public class TableWriterIntegrationTest extends JobIntegrationTest {
  @Rule
  public TemporaryFolder mTmpFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private TableWriter createWriter(AlluxioURI uri) throws IOException {
    TableSchema schema = new ParquetSchema(ParquetTestUtils.SCHEMA);
    return TableWriter.create(schema, uri);
  }

  @Test
  public void createLocalWriterWithNonExistingParents() throws IOException {
    File file = mTmpFolder.newFile();
    File parentDir = file.getParentFile();
    FileUtils.deleteDirectory(parentDir);
    AlluxioURI uri = new AlluxioURI("file:///" + file.toString());
    assertFalse(Files.exists(parentDir.toPath()));
    createWriter(uri).close();
    assertTrue(Files.exists(file.toPath()));
  }

  @Test
  public void createAlluxioWriterWithNonExistingParents() throws Exception {
    AlluxioURI uri = new AlluxioURI("alluxio",
        Authority.fromString(mFsContext.getMasterAddress().toString()),
        "/NON_EXISTENT_DIR/NON_EXISTENT_FILE");
    assertFalse(mFileSystem.exists(uri.getParent()));
    assertFalse(mFileSystem.exists(uri));
    createWriter(uri).close();
    assertTrue(mFileSystem.exists(uri));
  }

  @Test
  public void createWriterWithoutScheme() throws Exception {
    AlluxioURI uri = new AlluxioURI("/CREATE_WRITER_WITHOUT_SCHEME");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_SCHEME.getMessage(uri));
    createWriter(uri).close();
  }

  @Test
  public void createAlluxioWriterWithoutAuthority() throws Exception {
    AlluxioURI uri = new AlluxioURI("alluxio", null, "/CREATE_ALLUXIO_WRITER_WITHOUT_AUTHORITY");
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_AUTHORITY.getMessage(uri));
    createWriter(uri).close();
  }
}
