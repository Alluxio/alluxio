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
package tachyon.conf;

/**
 * Configurations shared by master and workers.
 */
public class CommonConf extends Utils {
  private static CommonConf COMMON_CONF = null;

  public final String TACHYON_HOME;
  public final String UNDERFS_ADDRESS;
  public final String UNDERFS_DATA_FOLDER;
  public final String UNDERFS_WORKERS_FOLDER;
  public final String UNDERFS_HDFS_IMPL;

  public final boolean USE_ZOOKEEPER;
  public final String ZOOKEEPER_ADDRESS;
  public final String ZOOKEEPER_ELECTION_PATH;
  public final String ZOOKEEPER_LEADER_PATH;

  private CommonConf() {
    TACHYON_HOME = getProperty("tachyon.home");
    UNDERFS_ADDRESS = getProperty("tachyon.underfs.address", TACHYON_HOME);
    UNDERFS_HDFS_IMPL = getProperty("tachyon.underfs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    UNDERFS_DATA_FOLDER = UNDERFS_ADDRESS + getProperty("tachyon.data.folder", "/tachyon/data");
    UNDERFS_WORKERS_FOLDER = 
        UNDERFS_ADDRESS + getProperty("tachyon.workers.folder", "/tachyon/workers");

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
  }

  public static synchronized CommonConf get() {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf();
    }

    return COMMON_CONF;
  }

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    COMMON_CONF = null;
  }
}
