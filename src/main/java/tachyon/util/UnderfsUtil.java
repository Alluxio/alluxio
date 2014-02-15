/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.PrefixList;
import tachyon.UnderFileSystem;
import tachyon.Version;
import tachyon.client.TachyonFS;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Utilities related to under filesystem
 */
public class UnderfsUtil {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public static void getInfo(TachyonFS tfs, String underfsAddress, String rootPath,
      PrefixList excludePathPrefix) throws IOException {
    LOG.info(tfs + " " + underfsAddress + " " + rootPath + " " + excludePathPrefix);

    Configuration tConf = new Configuration();
    tConf.set("fs.default.name", underfsAddress + rootPath);
    // TODO Use underfs to make this generic.
    UnderFileSystem fs = UnderFileSystem.get(underfsAddress);

    Queue<String> pathQueue = new LinkedList<String>();
    if (excludePathPrefix.outList(rootPath)) {
      pathQueue.add(underfsAddress + rootPath);
    }
    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();
      if (fs.isFile(path)) {
        String filePath = path.substring(underfsAddress.length());
        if (tfs.exist(filePath)) {
          LOG.info("File " + filePath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(filePath, path);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + filePath);
        } else {
          LOG.info("Create tachyon file " + filePath + " with file id " + fileId + " and "
              + "checkpoint location " + path);
        }
      } else {
        String[] files = fs.list(path);
        if (files != null) {
          for (String filePath : files) {
            LOG.info("Get: " + filePath);
            if (excludePathPrefix.outList(filePath)) {
              pathQueue.add(underfsAddress + filePath);
            }
          }
        }
        String filePath = path.substring(underfsAddress.length());
        if (!tfs.exist(filePath)) {
          tfs.mkdir(filePath);
        }
      }
    }
  }

  public static void main(String[] args)
      throws SuspectedFileSizeException, InvalidPathException, IOException,
      FileDoesNotExistException, FileAlreadyExistException, TException {
    if (!(args.length == 3 || args.length == 4)) {
      String prefix = "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " +
          "tachyon.util.UnderfsUtil ";
      System.out.println("Usage: " + prefix + "<TachyonAddress> <UnderfsAddress> <Path> " +
          "[<ExcludePathPrefix, separated by ;>]");
      System.out.println("Example: " + prefix +
          "tachyon://127.0.0.1:19998 hdfs://localhost:9000 / /tachyon");
      System.exit(-1);
    }

    PrefixList tExcludePathPrefix = null;
    if (args.length == 4) {
      tExcludePathPrefix = new PrefixList(args[3], ";");
    } else {
      tExcludePathPrefix = new PrefixList(null);
    }

    getInfo(TachyonFS.get(args[0]), args[1], args[2], tExcludePathPrefix);
    System.exit(0);
  }
}
