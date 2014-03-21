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
package tachyon.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;

/**
 * Master data image.
 */
public class Image {
  static final byte T_INVALID = -1;
  static final byte T_CHECKPOINT = 0;
  static final byte T_INODE_FILE = 1;
  static final byte T_INODE_FOLDER = 2;
  static final byte T_INODE_RAW_TABLE = 3;
  static final byte T_DEPENDENCY = 4;

  private final static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Write a new image to path. This method assumes having a lock on the master info.
   * 
   * @param info
   *          the master info to generate the image
   * @param path
   *          the new image path
   * @throws IOException
   */
  public static void create(MasterInfo info, String path) throws IOException {
    String tPath = path + ".tmp";
    String parentFolder = path.substring(0, path.lastIndexOf("/"));
    LOG.info("Creating the image file: " + tPath);
    UnderFileSystem ufs = UnderFileSystem.get(path);
    if (!ufs.exists(parentFolder)) {
      LOG.info("Creating parent folder " + parentFolder);
      ufs.mkdirs(parentFolder, true);
    }
    OutputStream os = ufs.create(tPath);
    DataOutputStream imageOs = new DataOutputStream(os);

    imageOs.writeInt(Constants.JOURNAL_VERSION);
    info.createImage(imageOs);
    imageOs.flush();
    imageOs.close();

    LOG.info("Succefully created the image file: " + tPath);
    ufs.delete(path, false);
    ufs.rename(tPath, path);
    ufs.delete(tPath, false);
    LOG.info("Renamed " + tPath + " to " + path);
    // safe to close, nothing created here with scope outside function
    ufs.close();
  }

  public static void load(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    if (!ufs.exists(path)) {
      LOG.info("Image " + path + " does not exist.");
      return;
    }
    LOG.info("Loading image " + path);
    DataInputStream imageIs = new DataInputStream(ufs.open(path));

    int tVersion = imageIs.readInt();
    if (tVersion != Constants.JOURNAL_VERSION) {
      throw new IOException("Image " + path + " has journal version " + tVersion + " ."
          + "The system has verion " + Constants.JOURNAL_VERSION);
    }

    info.loadImage(imageIs);
    imageIs.close();
    ufs.close();
  }

  public static void rename(String src, String dst) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(src);
    ufs.rename(src, dst);
    LOG.info("Renamed " + src + " to " + dst);
  }
}
