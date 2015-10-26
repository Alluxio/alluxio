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
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
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
  private final OutStreamOptions mClientOptions;
  private final int mNumbers = 20;

  public BasicOperations(TachyonURI masterLocation, TachyonURI filePath,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType) {
    mMasterLocation = masterLocation;
    mFilePath = filePath;
    mClientOptions = new OutStreamOptions.Builder(ClientContext.getConf())
        .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType).build();
  }

  @Override
  public Boolean call() throws Exception {
    TachyonConf tachyonConf = ClientContext.getConf();
    tachyonConf.set(Constants.MASTER_HOSTNAME, mMasterLocation.getHost());
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(mMasterLocation.getPort()));
    ClientContext.reset(tachyonConf);
    TachyonFileSystem tFS = TachyonFileSystem.TachyonFileSystemFactory.get();
    writeFile(tFS);
    return readFile(tFS);
  }

  private void writeFile(TachyonFileSystem tachyonFileSystem)
    throws IOException, TachyonException {
    ByteBuffer buf = ByteBuffer.allocate(mNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < mNumbers; k ++) {
      buf.putInt(k);
    }

    long startTimeMs = CommonUtils.getCurrentMs();
    FileOutStream os = tachyonFileSystem.getOutStream(mFilePath, mClientOptions);
    os.write(buf.array());
    os.close();

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "writeFile to file " + mFilePath));
  }

  private boolean readFile(TachyonFileSystem tachyonFileSystem)
      throws IOException, TachyonException {
    boolean pass = true;
    LOG.debug("Reading data...");
    TachyonFile file = tachyonFileSystem.open(mFilePath);
    InStreamOptions clientOptions = new InStreamOptions.Builder(ClientContext.getConf())
        .setTachyonStorageType(TachyonStorageType.STORE).build();
    final long startTimeMs = CommonUtils.getCurrentMs();
    FileInStream is = tachyonFileSystem.getInStream(file, clientOptions);
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
          + " <under storage type for writes (SYNC_PERSIST|NO_PERSIST)>");
      System.exit(-1);
    }

    Utils.runExample(new BasicOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        TachyonStorageType.valueOf(args[2]), UnderStorageType.valueOf(args[3])));
  }
}
