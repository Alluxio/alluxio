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
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.TachyonStorageType;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;

public class BasicOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final TachyonURI mMasterLocation;
  private final TachyonURI mFilePath;
  private final InStreamOptions mReadOptions;
  private final OutStreamOptions mWriteOptions;
  private final int mNumbers = 20;

  public BasicOperations(TachyonURI masterLocation, TachyonURI filePath, ReadType readType,
      WriteType writeType) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mReadOptions =
        new InStreamOptions.Builder(ClientContext.getConf()).setReadType(readType).build();
    mWriteOptions =
        new OutStreamOptions.Builder(ClientContext.getConf()).setWriteType(writeType).build();
  }

  @Override
  public Boolean call() throws Exception {
    TachyonConf tachyonConf = ClientContext.getConf();
    tachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort()));
    ClientContext.reset(tachyonConf);
    TachyonFileSystem tFS = TachyonFileSystem.TachyonFileSystemFactory.get();
    long fileId = createFile(tFS);
    writeFile(fileId);
    return readFile(tFS, fileId);
  }

  private long createFile(TachyonFileSystem tachyonFileSystem)
      throws IOException, TachyonException {
    LOG.debug("Creating file...");
    long startTimeMs = CommonUtils.getCurrentMs();
    CreateOptions createOptions =
        (new CreateOptions.Builder(ClientContext.getConf()))
            .setBlockSizeBytes(mWriteOptions.getBlockSizeBytes()).setRecursive(true)
            .setTTL(mWriteOptions.getTTL())
            .setUnderStorageType(mWriteOptions.getUnderStorageType()).build();
    TachyonFile tFile = tachyonFileSystem.create(mFilePath, createOptions);
    long fileId = tFile.getFileId();
    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "createFile with fileId " + fileId));
    return fileId;
  }

  private void writeFile(long fileId) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    long startTimeMs = CommonUtils.getCurrentMs();
    FileOutStream os = new FileOutStream(fileId, mWriteOptions);
    os.write(buf.array());
    os.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
  }

  private boolean readFile(TachyonFileSystem tachyonFileSystem, long fileId)
      throws IOException, TachyonException {
    boolean pass = true;
    LOG.debug("Reading data...");
    TachyonFile file = new TachyonFile(fileId);
    final long startTimeMs = CommonUtils.getCurrentMs();
    FileInStream is = tachyonFileSystem.getInStream(file, mReadOptions);
    ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
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
    if (args.length != 4) {
      System.out.println("java -cp " + Constants.TACHYON_JAR + " " + BasicOperations.class.getName()
          + " <ReadType (CACHE_PROMOTE | CACHE | NO_CACHE)> <WriteType (MUST_CACHE | CACHE_THROUGH"
          + " | THROUGH)>");
      System.exit(-1);
    }

    Utils.runExample(new BasicOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        ReadType.valueOf(args[2]), WriteType.valueOf(args[3])));
  }
}
