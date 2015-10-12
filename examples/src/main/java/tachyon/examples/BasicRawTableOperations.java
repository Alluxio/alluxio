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
import tachyon.client.table.RawColumn;
import tachyon.client.table.RawTable;
import tachyon.conf.TachyonConf;

public class BasicRawTableOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int COLS = 3;

  private final TachyonURI mMasterAddress;
  private final TachyonURI mTablePath;
  private final WriteType mWriteType;
  private final int mDataLength = 20;
  private final int mMetadataLength = 5;
  private long mId;

  public BasicRawTableOperations(TachyonURI masterAddress, TachyonURI tablePath,
      WriteType writeType) {
    mMasterAddress = masterAddress;
    mTablePath = tablePath;
    mWriteType = writeType;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mMasterAddress, new TachyonConf());
    createRawTable(tachyonClient);
    write(tachyonClient);
    return read(tachyonClient);
  }

  private void createRawTable(TachyonFS tachyonClient) throws IOException {
    ByteBuffer data = ByteBuffer.allocate(mMetadataLength * 4);
    data.order(ByteOrder.nativeOrder());
    for (int k = -mMetadataLength; k < 0; k ++) {
      data.putInt(k);
    }
    data.flip();
    mId = tachyonClient.createRawTable(mTablePath, 3, data);
  }

  private boolean read(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;

    LOG.debug("Reading data...");
    RawTable rawTable = tachyonClient.getRawTable(mId);
    ByteBuffer metadata = rawTable.getMetadata();
    LOG.debug("Metadata: ");
    metadata.order(ByteOrder.nativeOrder());
    for (int k = -mMetadataLength; k < 0; k ++) {
      pass = pass && (metadata.getInt() == k);
    }

    for (int column = 0; column < COLS; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      TachyonFile tFile = rawColumn.getPartition(0);
      FileInStream is = tFile.getInStream(ReadType.CACHE);
      ByteBuffer buf = ByteBuffer.allocate((int) tFile.getBlockSizeByte());
      is.read(buf.array());
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mDataLength; k ++) {
        pass = pass && (buf.getInt() == k);
      }
      is.close();
    }
    return pass;
  }

  private void write(TachyonFS tachyonClient) throws IOException {
    RawTable rawTable = tachyonClient.getRawTable(mTablePath);

    LOG.debug("Writing data...");
    for (int column = 0; column < COLS; column ++) {
      RawColumn rawColumn = rawTable.getRawColumn(column);
      if (!rawColumn.createPartition(0)) {
        throw new IOException("Failed to create partition in table " + mTablePath
            + " under column " + column);
      }

      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mDataLength; k ++) {
        buf.putInt(k);
      }
      buf.flip();

      TachyonFile tFile = rawColumn.getPartition(0);
      FileOutStream os = tFile.getOutStream(mWriteType);
      os.write(buf.array());
      os.close();
    }
  }

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length != 3) {
      System.out.println("java -cp " + Constants.TACHYON_JAR
          + " tachyon.examples.BasicRawTableOperations <TachyonMasterAddress> <FilePath>");
      System.exit(-1);
    }
    Utils.runExample(new BasicRawTableOperations(new TachyonURI(args[0]), new TachyonURI(args[1]),
        WriteType.valueOf(args[2])));
  }
}
