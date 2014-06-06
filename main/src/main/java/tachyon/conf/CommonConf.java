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
package tachyon.conf;

import java.io.File;

import org.apache.log4j.Logger;

import tachyon.Constants;

/**
 * Configurations shared by master and workers.
 */
public class CommonConf extends Utils {
  private static final Logger LOG = Logger.getLogger("");

  private static CommonConf COMMON_CONF = null;

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    COMMON_CONF = null;
  }

  /***
   * get configuration from command line options 
   * @return
   */
  public static synchronized CommonConf get() {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf(null);
    }

    return COMMON_CONF;
  }

  /***
   * get configuration from configuration file
   * @param name configuration file
   * @return
   */
  public static synchronized CommonConf get(String name) {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf(name);
    }

    return COMMON_CONF;
  }

  public final String TACHYON_HOME;
  public final String UNDERFS_ADDRESS;
  public final String UNDERFS_DATA_FOLDER;
  public final String UNDERFS_WORKERS_FOLDER;

  public final String UNDERFS_HDFS_IMPL;
  public final String UNDERFS_GLUSTERFS_IMPL;
  public final String UNDERFS_GLUSTERFS_VOLUMES;
  public final String UNDERFS_GLUSTERFS_MOUNTS;
  public final String UNDERFS_GLUSTERFS_MR_DIR;
  public final String WEB_RESOURCES;
  public final boolean USE_ZOOKEEPER;
  public final String ZOOKEEPER_ADDRESS;

  public final String ZOOKEEPER_ELECTION_PATH;

  public final String ZOOKEEPER_LEADER_PATH;

  public final boolean ASYNC_ENABLED;

  public final int MAX_COLUMNS;

  public final int MAX_TABLE_METADATA_BYTE;

  private CommonConf(String name) {
    if (name != null)
      addResource(name);
    
    if (System.getProperty("tachyon.home") == null) {
      LOG.warn("tachyon.home is not set. Using /mnt/tachyon_default_home as the default value.");
      File file = new File("/mnt/tachyon_default_home");
      if (!file.exists()) {
        file.mkdirs();
      }
    }
    TACHYON_HOME = getProperty("tachyon.home", "/mnt/tachyon_default_home");
    WEB_RESOURCES = getProperty("tachyon.web.resources", TACHYON_HOME + "/main/src/main/webapp");
    UNDERFS_ADDRESS = getProperty("tachyon.underfs.address", TACHYON_HOME + "/underfs");
    UNDERFS_DATA_FOLDER = getProperty("tachyon.data.folder", UNDERFS_ADDRESS + "/tachyon/data");
    UNDERFS_WORKERS_FOLDER =
        getProperty("tachyon.workers.folder", UNDERFS_ADDRESS + "/tachyon/workers");
    UNDERFS_HDFS_IMPL =
        getProperty("tachyon.underfs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    UNDERFS_GLUSTERFS_IMPL =
        getProperty("tachyon.underfs.glusterfs.impl", "org.apache.hadoop.fs.glusterfs.GlusterFileSystem");
    UNDERFS_GLUSTERFS_VOLUMES = 
        getProperty("tachyon.underfs.glusterfs.volumes", null);
    UNDERFS_GLUSTERFS_MOUNTS = 
        getProperty("tachyon.underfs.glusterfs.mounts", null);
    UNDERFS_GLUSTERFS_MR_DIR = 
        getProperty("tachyon.underfs.glusterfs.mapred.system.dir", "glusterfs:///mapred/system");
    USE_ZOOKEEPER = getBooleanProperty("tachyon.usezookeeper", false);
    if (USE_ZOOKEEPER) {
      ZOOKEEPER_ADDRESS = getProperty("tachyon.zookeeper.address");
      ZOOKEEPER_ELECTION_PATH = getProperty("tachyon.zookeeper.election.path", "/election");
      ZOOKEEPER_LEADER_PATH = getProperty("tachyon.zookeeper.leader.path", "/leader");
    } else {
      ZOOKEEPER_ADDRESS = null;
      ZOOKEEPER_ELECTION_PATH = null;
      ZOOKEEPER_LEADER_PATH = null;
    }

    ASYNC_ENABLED = getBooleanProperty("tachyon.async.enabled", false);

    MAX_COLUMNS = getIntProperty("tachyon.max.columns", 1000);
    MAX_TABLE_METADATA_BYTE = getIntProperty("tachyon.max.table.metadata.byte", Constants.MB * 5);
  }
}
