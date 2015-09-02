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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.client.next.file.FileInStream;
import tachyon.conf.TachyonConf;
import tachyon.master.filesystem.meta.DependencyType;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;

/**
 * An example to show to how use Tachyon's API
 */
public class BasicCheckpoint implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mLocation;
  private final String mFileFolder;
  private final int mNumFiles;

  public BasicCheckpoint(TachyonURI tachyonURI, String fileFolder, int numFiles) {
    mLocation = tachyonURI;
    mFileFolder = fileFolder;
    mNumFiles = numFiles;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonFS tachyonClient = TachyonFS.get(mLocation, new TachyonConf());
    createDependency(tachyonClient);
    writeFile(tachyonClient);
    return readFile(tachyonClient);
  }

  private void createDependency(TachyonFS tachyonClient) throws IOException {
    long startTimeMs = CommonUtils.getCurrentMs();
    List<String> children = new ArrayList<String>();
    for (int k = 0; k < mNumFiles; k ++) {
      children.add(mFileFolder + "/part-" + k);
    }
    List<ByteBuffer> data = new ArrayList<ByteBuffer>();
    data.add(ByteBuffer.allocate(10));
    int depId =
        tachyonClient.createDependency(new ArrayList<String>(), children, "fake command", data,
            "BasicCheckpoint Dependency", "Tachyon Examples", "0.3",
            DependencyType.Narrow.getValue(), 512 * Constants.MB);

    LOG.info(FormatUtils.formatTimeTakenMs(startTimeMs, "createDependency with depId " + depId));
  }

  private boolean readFile(TachyonFS tachyonClient) throws IOException {
    boolean pass = true;
    for (int i = 0; i < mNumFiles; i ++) {
      TachyonURI filePath = new TachyonURI(mFileFolder + "/part-" + i);
      LOG.debug("Reading data from {}", filePath);
      TachyonFile file = tachyonClient.getFile(filePath);
      FileInStream is = file.getInStream(ReadType.CACHE);
      ByteBuffer buf = ByteBuffer.allocate((int) file.getBlockSizeByte());
      is.read(buf.array());
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mNumFiles; k ++) {
        pass = pass && (buf.getInt() == k);
      }
      is.close();
    }
    return pass;
  }

  private void writeFile(TachyonFS tachyonClient) throws IOException {
    for (int i = 0; i < mNumFiles; i ++) {
      ByteBuffer buf = ByteBuffer.allocate(80);
      buf.order(ByteOrder.nativeOrder());
      for (int k = 0; k < mNumFiles; k ++) {
        buf.putInt(k);
      }
      buf.flip();
      TachyonURI filePath = new TachyonURI(mFileFolder + "/part-" + i);
      LOG.debug("Writing data to {}", filePath);
      TachyonFile file = tachyonClient.getFile(filePath);
      OutputStream os = file.getOutStream(WriteType.ASYNC_THROUGH);
      os.write(buf.array());
      os.close();
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar "
          + "tachyon.examples.BasicCheckpoint <TachyonMasterAddress> <FileFolder> <Files>");
      System.exit(-1);
    }

    Utils.runExample(new BasicCheckpoint(new TachyonURI(args[0]), args[1], Integer
        .parseInt(args[2])));
  }
}
