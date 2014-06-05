/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.Version;
import tachyon.client.OutStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.util.CommonUtils;

public class BasicOperations {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private static TachyonFS sTachyonClient;
  private static String sFilePath = null;
  private static WriteType sWriteType = null;
  private static int sNumbers = 20;
  private static boolean sPass = true;

  public static void createFile() throws IOException {
    long startTimeMs = CommonUtils.getCurrentMs();
    int fileId = sTachyonClient.createFile(sFilePath);
    CommonUtils.printTimeTakenMs(startTimeMs, LOG, "createFile with fileId " + fileId);
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicOperations <TachyonMasterAddress> <FilePath> <WriteType>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sFilePath = args[1];
    sWriteType = WriteType.getOpType(args[2]);
    createFile();
    writeFile();
    readFile();
    Utils.printPassInfo(sPass);
    System.exit(0);
  }

  public static void readFile() throws IOException {
    LOG.debug("Reading data...");
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    TachyonByteBuffer buf = file.readByteBuffer();
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer();
    }
    buf.DATA.order(ByteOrder.nativeOrder());
    for (int k = 0; k < sNumbers; k ++) {
      sPass = sPass && (buf.DATA.getInt() == k);
    }
    buf.close();
  }

  public static void writeFile() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(sNumbers * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < sNumbers; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    LOG.debug("Writing data...");
    buf.flip();

    TachyonFile file = sTachyonClient.getFile(sFilePath);
    OutStream os = file.getOutStream(sWriteType);
    os.write(buf.array());
    os.close();
  }
}