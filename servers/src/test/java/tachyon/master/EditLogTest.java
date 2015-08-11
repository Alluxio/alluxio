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

package tachyon.master;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * Unit Test for EditLog. Test the read/write correctness of each operation.
 */
public class EditLogTest {
  private EditLog mEditLog = null;
  private TachyonConf mTachyonConf = new TachyonConf();
  private String mTmpPath = null;

  @Test
  public void addBlockTest() throws IOException {
    // Write ADD_BLOCK
    mEditLog.addBlock(1, 2, 128L, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read ADD_BLOCK and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.ADD_BLOCK, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertEquals(2, op.getInt("blockIndex").intValue());
    Assert.assertEquals(128L, op.getLong("blockLength").longValue());
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void addCheckpointTest() throws IOException {
    // Write ADD_CHECKPOINT
    TachyonURI checkpointPath = new TachyonURI("/test/checkpoint");
    mEditLog.addCheckpoint(1, 256L, checkpointPath, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read ADD_CHECKPOINT and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.ADD_CHECKPOINT, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertEquals(256L, op.getLong("length").longValue());
    Assert.assertEquals(checkpointPath.toString(), op.getString("path"));
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  @After
  public final void after() throws Exception {
    new File(mTmpPath).delete();
  }

  @Before
  public final void before() throws Exception {
    mTmpPath = File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mEditLog = new EditLog(mTmpPath, false, 100, mTachyonConf);
  }

  @Test
  public void completeFileTest() throws IOException {
    // Write COMPLETE_FILE
    mEditLog.completeFile(1, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read COMPLETE_FILE and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.COMPLETE_FILE, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void createDependencyTest() throws IOException {
    // Write CREATE_DEPENDENCY
    List<Integer> parents = Arrays.asList(1, 2, 3);
    List<Integer> children = Arrays.asList(4, 5, 6, 7);
    List<ByteBuffer> data = Arrays.asList(ByteBuffer.wrap(Base64.decodeBase64("AAAAAAAAAAAAA==")));
    mEditLog.createDependency(parents, children, "fake command", data, "Comment Test",
        "Tachyon Examples", "0.3", DependencyType.Narrow, 1, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_DEPENDENCY and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_DEPENDENCY, op.mType);
    Assert.assertEquals(parents, op.get("parents", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(children, op.get("children", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals("fake command", op.getString("commandPrefix"));
    Assert.assertEquals(data, op.getByteBufferList("data"));
    Assert.assertEquals("Comment Test", op.getString("comment"));
    Assert.assertEquals("Tachyon Examples", op.getString("framework"));
    Assert.assertEquals("0.3", op.getString("frameworkVersion"));
    Assert.assertEquals(DependencyType.Narrow, op.get("dependencyType", DependencyType.class));
    Assert.assertEquals(1, op.getInt("dependencyId").intValue());
    Assert.assertEquals(1409349750338L, op.getLong("creationTimeMs").longValue());
  }

  @Test
  public void createFileTest() throws IOException {
    // Write CREATE_FILE
    TachyonURI createFilePath = new TachyonURI("/test/createFilePath");
    mEditLog.createFile(true, createFilePath, false, 128L, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_FILE and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_FILE, op.mType);
    Assert.assertTrue(op.getBoolean("recursive"));
    Assert.assertEquals(createFilePath.toString(), op.getString("path"));
    Assert.assertFalse(op.getBoolean("directory"));
    Assert.assertEquals(128L, op.getLong("blockSizeByte").longValue());
    Assert.assertEquals(1409349750338L, op.getLong("creationTimeMs").longValue());
  }

  @Test
  public void createRawTableTest() throws IOException {
    // Write CREATE_RAW_TABLE
    ByteBuffer metadata = ByteBuffer.wrap(Base64.decodeBase64("BBBBBBBBBBBBB++"));
    mEditLog.createRawTable(1, 2, metadata);
    mEditLog.flush();
    mEditLog.close();

    // Read CREATE_RAW_TABLE and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.CREATE_RAW_TABLE, op.mType);
    Assert.assertEquals(1, op.getInt("tableId").intValue());
    Assert.assertEquals(2, op.getInt("columns").intValue());
    Assert.assertEquals(Base64.encodeBase64String(metadata.array()), op.getString("metadata"));
  }

  @Test
  public void deleteTest() throws IOException {
    // Write DELETE
    mEditLog.delete(1, true, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read DELETE and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.DELETE, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertTrue(op.getBoolean("recursive"));
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  private EditLogOperation getSingleOpFromFile(String path) throws IOException {
    DataInputStream is = new DataInputStream(new FileInputStream(path));
    JsonParser parser = JsonObject.createObjectMapper().getFactory().createParser(is);
    EditLogOperation ret = parser.readValueAs(EditLogOperation.class);
    is.close();
    return ret;
  }

  @Test
  public void renameTest() throws IOException {
    // Write RENAME
    TachyonURI renamePath = new TachyonURI("/test/renamePath");
    mEditLog.rename(1, renamePath, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read RENAME and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.RENAME, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertEquals(renamePath.toString(), op.getString("dstPath"));
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void setPinnedTest() throws IOException {
    // Write SET_PINNED
    mEditLog.setPinned(1, true, 1409349750338L);
    mEditLog.flush();
    mEditLog.close();

    // Read SET_PINNED and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.SET_PINNED, op.mType);
    Assert.assertEquals(1, op.getInt("fileId").intValue());
    Assert.assertTrue(op.getBoolean("pinned"));
    Assert.assertEquals(1409349750338L, op.getLong("opTimeMs").longValue());
  }

  @Test
  public void updateRawTableMetadataTest() throws IOException {
    // Write UPDATE_RAW_TABLE_METADATA
    ByteBuffer metadata = ByteBuffer.wrap(Base64.decodeBase64("CCCCCCCCCCCCC--"));
    mEditLog.updateRawTableMetadata(1, metadata);
    mEditLog.flush();
    mEditLog.close();

    // Read UPDATE_RAW_TABLE_METADATA and check
    EditLogOperation op = getSingleOpFromFile(mTmpPath);
    Assert.assertEquals(101, op.mTransId);
    Assert.assertEquals(EditLogOperationType.UPDATE_RAW_TABLE_METADATA, op.mType);
    Assert.assertEquals(1, op.getInt("tableId").intValue());
    Assert.assertEquals(Base64.encodeBase64String(metadata.array()), op.getString("metadata"));
  }
}
