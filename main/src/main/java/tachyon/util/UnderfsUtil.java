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

    /**
     * keep this signature so as not to invalidate existing code referring getInfo/4
      */
  public static void getInfo(TachyonFS tfs, String underfsAddress, String rootPath,
      PrefixList excludePathPrefix) throws IOException {
      getInfo(tfs, "/", underfsAddress, rootPath, excludePathPrefix);
  }

    /**
     * This getInfo/5 signature introduces an extra parameter tfsRoot, like a mounting point in TFS.
     * Files under rootPath will be all registered under tachyon::/host:port/tfsRoot/rootPath.
     * @param tfs the TFS handler created out of address like "tachyon://host:port"
     * @param tfsRoot the destination point in TFS to load the under FS path onto
     * @param underfsAddress the address of the under FS server, like "hdfs://h:p", or "" for local FS.
     * @param rootPath the source path in the under FS, like "/dir".
     * @param excludePathPrefix paths to exclude from rootPath, which will not be registered in TFS.
     * @throws IOException
     */
  public static void getInfo(TachyonFS tfs, String tfsRoot, String underfsAddress, String rootPath,
              PrefixList excludePathPrefix) throws IOException {
    LOG.info(tfs + tfsRoot + " " + underfsAddress + rootPath + " " + excludePathPrefix);

    if (!tfs.exist(tfsRoot)) {
        tfs.mkdir(tfsRoot);
        LOG.info("directory "+tfsRoot+" does not exist in Tachyon: created");
    }

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
        String tfsPath = (tfsRoot + filePath).replace("//", "/");
        if (tfs.exist(tfsPath)) {
          LOG.info("File " + tfsPath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(tfsPath, path);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + tfsPath);
        } else {
          LOG.info("Create tachyon file " + tfsPath + " with file id " + fileId + " and "
              + "checkpoint location " + path);
        }
      } else { // isDirectory(path)
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
        String tfsPath = (tfsRoot + filePath).replace("//", "/");
        if (!tfs.exist(tfsPath)) {
          tfs.mkdir(tfsPath);
        }
      }
    }
  }

  public static void main(String[] args) throws SuspectedFileSizeException, InvalidPathException,
      IOException, FileDoesNotExistException, FileAlreadyExistException, TException {

    if (!(args.length == 2 || args.length == 3)) {
      String cmd =
          "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
              + "tachyon.util.UnderfsUtil ";
      //cmd = "bin/tachyon loadufs ";

      System.out.println("Usage: " + cmd + "<TachyonPath> <UnderfsPath> "
          + "[<Optional ExcludePathPrefix, separated by ;>]");
      System.out.println("Example: " + cmd
          + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b /c");
      System.out.println("Example: " + cmd
          + "tachyon://127.0.0.1:19998/a file:///b /c");
      System.out.println("Example: " + cmd
          + "tachyon://127.0.0.1:19998/a /b /c");
      System.out.println("In the TFS, files will take path /a/b, excluding underFS files /c from /b");

      System.exit(-1);
    }

    PrefixList tExcludePathPrefix = null;
    if (args.length == 3) {
      tExcludePathPrefix = new PrefixList(args[2], ";");
    } else {
      tExcludePathPrefix = new PrefixList(null);
    }

    // parse the given TachyonPath into a prefixing TFS address and a rootPath
    String[] tfsPair = UnderFileSystem.parse(args[0]);

    // parse the given UnderfsPath into a prefixing UnderfsAddress and a rootPath
    String[] fsPair = UnderFileSystem.parse(args[1]);

    // so that the original API is reserved
    getInfo(TachyonFS.get(tfsPair[0]), tfsPair[1], fsPair[0], fsPair[1], tExcludePathPrefix);
    System.exit(0);
  }
}
