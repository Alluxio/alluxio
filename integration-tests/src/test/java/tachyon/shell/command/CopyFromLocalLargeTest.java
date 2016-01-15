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

package tachyon.shell.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.client.TachyonStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.thrift.FileInfo;
import tachyon.util.io.BufferUtils;

/**
 * Test used to copy a large file from local
 */
public class CopyFromLocalLargeTest extends AbstractTfsShellTest {
  @Test
  public void copyFromLocalLargeTest() throws IOException, TachyonException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + "/testFile");
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    byte[] toWrite = BufferUtils.getIncreasingByteArray(SIZE_BYTES);
    fos.write(toWrite);
    fos.close();
    mFsShell.run("copyFromLocal", testFile.getAbsolutePath(), "/testFile");
    Assert.assertEquals(getCommandOutput(new String[] {"copyFromLocal", testFile.getAbsolutePath(),
        "/testFile"}), mOutput.toString());
    TachyonFile tFile = mTfs.open(new TachyonURI("/testFile"));
    FileInfo fileInfo = mTfs.getInfo(tFile);
    Assert.assertNotNull(fileInfo);
    Assert.assertEquals(SIZE_BYTES, fileInfo.length);

    InStreamOptions options =
        new InStreamOptions.Builder(new TachyonConf()).setTachyonStorageType(
            TachyonStorageType.NO_STORE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[SIZE_BYTES];
    tfis.read(read);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(SIZE_BYTES, read));
  }
}
