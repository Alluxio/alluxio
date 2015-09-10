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

package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;

public class BasicOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final WriteType mWriteType;
  private final int mNumbers = 20;

  public BasicOperations(TachyonURI masterLocation, TachyonURI filePath, WriteType writeType) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mWriteType = writeType;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterLocation, new TachyonConf());
    createFile(tachyonClient);
    writeFile(tachyonClient);
    return readFile(tachyonClient);
  }

  private void createFile(TachyonFS tachyonClient) throws IOException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    long fileId = tachyonClient.createFile(mFilePath);
    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "createFile with fileId " + fileId));
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    FileOutStream os = file.getOutStream(mWriteType);
    os.write(buf.array());
    os.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
  }

  private boolean readFile(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    LOG.debug("Reading data...");

    final long startTimeMs = CommonUtils.getCurrentMs();
    TachyonFile file = tachyonClient.getFile(mFilePath);
    FileInStream is = file.getInStream(ReadType.CACHE);
    ByteBuffer buf = ByteBuffer.allocate((int) file.getBlockSizeByte());
    is.read(buf.array());
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      pass = pass && (buf.getInt() == k);
    }
    is.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "readFile file " + mFilePath));
    return pass;
  }

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    }

    Utils.runExample(new BasicOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        WriteType.valueOf(args[2])));
  }
}
