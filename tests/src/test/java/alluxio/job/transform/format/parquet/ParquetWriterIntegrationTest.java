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

package alluxio.job.transform.format.parquet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.job.JobIntegrationTest;
import alluxio.job.transform.format.TableSchema;
import alluxio.uri.Authority;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit tests for {@link ParquetWriter}.
 */
public class ParquetWriterIntegrationTest extends JobIntegrationTest {
  @Test
  public void createLocalFileWithNonExistingParents() throws IOException {
    Path path = Files.createTempFile("local", "parquet");
    Files.delete(path);
    AlluxioURI uri = new AlluxioURI("file:///" + path.toString());

    assertFalse(Files.exists(path));
    TableSchema schema = new ParquetSchema(ParquetTestUtils.SCHEMA);
    ParquetWriter writer = ParquetWriter.create(schema, uri);
    writer.close();
    assertTrue(Files.exists(path));
  }

  @Test
  public void createAlluxioFileWithNonExistingParents() throws Exception {
    AlluxioURI uri = new AlluxioURI("alluxio",
        Authority.fromString(mFsContext.getMasterAddress().toString()),
        "/NON_EXISTENT_DIR/NON_EXISTENT_FILE");

    assertFalse(mFileSystem.exists(uri.getParent()));
    assertFalse(mFileSystem.exists(uri));
    TableSchema schema = new ParquetSchema(ParquetTestUtils.SCHEMA);
    ParquetWriter writer = ParquetWriter.create(schema, uri);
    writer.close();
    assertTrue(mFileSystem.exists(uri));
  }
}
