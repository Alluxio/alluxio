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
package tachyon.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.Constants;
import tachyon.UnderFileSystem;

/**
 * Master data image.
 */
public class Image {
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
    String parentFolder = path.substring(0, path.lastIndexOf(Constants.PATH_SEPARATOR));
    LOG.info("Creating the image file: " + tPath);
    UnderFileSystem ufs = UnderFileSystem.get(path);
    DataOutputStream imageOs = null;
    try {
      if (!ufs.exists(parentFolder)) {
        LOG.info("Creating parent folder " + parentFolder);
        ufs.mkdirs(parentFolder, true);
      }
      OutputStream os = ufs.create(tPath);
      imageOs = new DataOutputStream(os);
      ObjectWriter writer = JsonObject.createObjectMapper().writer();

      info.writeImage(writer, imageOs);
      imageOs.flush();

      LOG.info("Succefully created the image file: " + tPath);
      ufs.delete(path, false);
      ufs.rename(tPath, path);
      ufs.delete(tPath, false);
      LOG.info("Renamed " + tPath + " to " + path);

    } finally {
      boolean ufsClosed = false;
      if (imageOs != null) {
        try {
          imageOs.close();
        } catch (IOException e) {
          ufs.close();
          ufsClosed = true;
          //TODO: Do we need to throw this IOException if ufs.close() succeeded?
          //We can save this IOException and throw after line # 83
        }
      }
      if (!ufsClosed) {
        ufs.close();
      }
    }
  }

  /**
   * Load an image into the masterinfo.
   * 
   * @param info
   *          the masterinfo to fill.
   * @param path
   *          the data to load
   * @throws IOException
   */
  public static void load(MasterInfo info, String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    DataInputStream imageIs = null;
    try {
      if (!ufs.exists(path)) {
        LOG.info("Image " + path + " does not exist.");
        return;
      }
      LOG.info("Loading image " + path);
      imageIs = new DataInputStream(ufs.open(path));
      JsonParser parser = JsonObject.createObjectMapper().getFactory().createParser(imageIs);
      info.loadImage(parser, path);
    } finally {
      boolean ufsClosed = false;
      if (imageIs != null) {
        try {
          imageIs.close();
        } catch (IOException e) {
          ufs.close();
          ufsClosed = true;
          //TODO: Do we need to throw this IOException if ufs.close() succeeded?
          //We can save this IOException and throw after line # 122
        }
      }
      if (!ufsClosed) {
        ufs.close();
      }
    }
  }

  /**
   * Rename the src to the dst. Only used to rename the Image.
   * 
   * @param src
   *          The src image path
   * @param dst
   *          The dst image path
   * @throws IOException
   */
  public static void rename(String src, String dst) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(src);
    ufs.rename(src, dst);
    LOG.info("Renamed " + src + " to " + dst);
  }
}
