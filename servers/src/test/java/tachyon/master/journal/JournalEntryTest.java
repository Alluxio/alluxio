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

package tachyon.master.journal;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.file.meta.DependencyType;

/**
 * Unit Test for journal entries. Test the read/write correctness of each operation.
 */
public class JournalEntryTest {
  /*
  private static final int TEST_FILE_ID = 1;
  private static final long TEST_OP_TIME_MS = 1409349750338L;
  private static final int TEST_TABLE_ID = 2;
  private static final long TEST_TRANSACTION_ID = 100L;

  private EditLog mEditLog = null;
  private String mEditLogPath = null;
  private TachyonConf mTachyonConf = new TachyonConf();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public final void before() throws Exception {
    mEditLogPath = mTestFolder.newFile().getAbsolutePath();
    mEditLog = new EditLog(mEditLogPath, false, TEST_TRANSACTION_ID, mTachyonConf);
  }

  private EditLogOperation getSingleOpFromFile(String path) throws IOException {
    DataInputStream is = new DataInputStream(new FileInputStream(path));
    JsonParser parser = JsonObject.createObjectMapper().getFactory().createParser(is);
    EditLogOperation ret = parser.readValueAs(EditLogOperation.class);
    is.close();
    return ret;
  }

  @Test
  public void addBlockTest() throws IOException {
    // Write ADD_BLOCK
    int blockIndex = 2;
    long blockLength = 100L;
    mEditLog.addBlock(TEST_FILE_ID, blockIndex, blockLength, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read ADD_BLOCK and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.ADD_BLOCK, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertEquals(blockIndex, op.getInt("blockIndex").intValue());
    Assert.assertEquals(blockLength, op.getLong("blockLength").longValue());
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void addCheckpointTest() throws IOException {
    // Write ADD_CHECKPOINT
    long length = 256L;
    TachyonURI checkpointPath = new TachyonURI("/test/checkpoint");
    mEditLog.addCheckpoint(TEST_FILE_ID, length, checkpointPath, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read ADD_CHECKPOINT and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.ADD_CHECKPOINT, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertEquals(length, op.getLong("length").longValue());
    Assert.assertEquals(checkpointPath.toString(), op.getString("path"));
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void completeFileTest() throws IOException {
    // Write COMPLETE_FILE
    mEditLog.completeFile(TEST_FILE_ID, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read COMPLETE_FILE and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.COMPLETE_FILE, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void createDependencyTest() throws IOException {
    // Write CREATE_DEPENDENCY
    List<Integer> parents = Arrays.asList(1, 2, 3);
    List<Integer> children = Arrays.asList(4, 5, 6, 7);
    String commandPrefix = "fake command";
    List<ByteBuffer> data = Arrays.asList(ByteBuffer.wrap(Base64.decodeBase64("AAAAAAAAAAAAA==")));
    String comment = "Comment Test";
    String framework = "Tachyon Examples";
    String frameworkVersion = "0.3";
    DependencyType dependencyType = DependencyType.Narrow;
    int depId = 1;
    mEditLog.createDependency(parents, children, commandPrefix, data, comment, framework,
        frameworkVersion, dependencyType, depId, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_DEPENDENCY and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_DEPENDENCY, op.mType);
    Assert.assertEquals(parents, op.get("parents", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(children, op.get("children", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(commandPrefix, op.getString("commandPrefix"));
    Assert.assertEquals(data, op.getByteBufferList("data"));
    Assert.assertEquals(comment, op.getString("comment"));
    Assert.assertEquals(framework, op.getString("framework"));
    Assert.assertEquals(frameworkVersion, op.getString("frameworkVersion"));
    Assert.assertEquals(dependencyType, op.get("dependencyType", DependencyType.class));
    Assert.assertEquals(depId, op.getInt("dependencyId").intValue());
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("creationTimeMs").longValue());
  }

  @Test
  public void createFileTest() throws IOException {
    // Write CREATE_FILE
    long blockSizeBytes = 128L;
    TachyonURI createFilePath = new TachyonURI("/test/createFilePath");
    mEditLog.createFile(true, createFilePath, false, blockSizeBytes, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_FILE and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_FILE, op.mType);
    Assert.assertTrue(op.getBoolean("recursive"));
    Assert.assertEquals(createFilePath.toString(), op.getString("path"));
    Assert.assertFalse(op.getBoolean("directory"));
    Assert.assertEquals(blockSizeBytes, op.getLong("blockSizeBytes").longValue());
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("creationTimeMs").longValue());
  }

  @Test
  public void createRawTableTest() throws IOException {
    // Write CREATE_RAW_TABLE
    int columns = 2;
    ByteBuffer metadata = ByteBuffer.wrap(Base64.decodeBase64("BBBBBBBBBBBBB++"));
    mEditLog.createRawTable(TEST_FILE_ID, columns, metadata);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_RAW_TABLE and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_RAW_TABLE, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("tableId").intValue());
    Assert.assertEquals(columns, op.getInt("columns").intValue());
    Assert.assertEquals(Base64.encodeBase64String(metadata.array()), op.getString("metadata"));
  }

  @Test
  public void deleteTest() throws IOException {
    // Write DELETE
    mEditLog.delete(TEST_FILE_ID, true, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read DELETE and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.DELETE, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertTrue(op.getBoolean("recursive"));
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void renameTest() throws IOException {
    // Write RENAME
    TachyonURI renamePath = new TachyonURI("/test/renamePath");
    mEditLog.rename(TEST_FILE_ID, renamePath, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read RENAME and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.RENAME, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertEquals(renamePath.toString(), op.getString("dstPath"));
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void setPinnedTest() throws IOException {
    // Write SET_PINNED
    mEditLog.setPinned(TEST_FILE_ID, true, TEST_OP_TIME_MS);
    mEditLog.flush();
    mEditLog.close();

    // Read SET_PINNED and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.SET_PINNED, op.mType);
    Assert.assertEquals(TEST_FILE_ID, op.getInt("fileId").intValue());
    Assert.assertTrue(op.getBoolean("pinned"));
    Assert.assertEquals(TEST_OP_TIME_MS, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void updateRawTableMetadataTest() throws IOException {
    // Write UPDATE_RAW_TABLE_METADATA
    ByteBuffer metadata = ByteBuffer.wrap(Base64.decodeBase64("CCCCCCCCCCCCC--"));
    mEditLog.updateRawTableMetadata(TEST_TABLE_ID, metadata);
    mEditLog.flush();
    mEditLog.close();

    // Read UPDATE_RAW_TABLE_METADATA and check
    EditLogOperation op = getSingleOpFromFile(mEditLogPath);
    Assert.assertEquals(TEST_TRANSACTION_ID + 1, op.mTransId);
    Assert.assertEquals(EditLogOperationType.UPDATE_RAW_TABLE_METADATA, op.mType);
    Assert.assertEquals(TEST_TABLE_ID, op.getInt("tableId").intValue());
    Assert.assertEquals(Base64.encodeBase64String(metadata.array()), op.getString("metadata"));
  }
  */
}
