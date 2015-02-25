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

package tachyon.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterInfo;

/**
 * Servlet that provides data for displaying the system's configuration.
 */
public final class WebInterfaceConfigurationServlet extends HttpServlet {
  private static final long serialVersionUID = 2134205675393443914L;

  private final transient MasterInfo mMasterInfo;
  private final TachyonConf mTachyonConf;

  public WebInterfaceConfigurationServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
    mTachyonConf = new TachyonConf();
  }

  /**
   * Populates attributes before redirecting to a jsp.
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("whitelist", mMasterInfo.getWhiteList());

    // CommonConf
    request.setAttribute("tachyon.home", "" + mTachyonConf.get(Constants.TACHYON_HOME, ""));
    request.setAttribute("tachyon.underfs.address", ""
        + mTachyonConf.get(Constants.UNDERFS_ADDRESS, ""));
    request.setAttribute("tachyon.data.folder", ""
        + mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, ""));
    request.setAttribute("tachyon.workers.folder", ""
        + mTachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER, ""));
    request.setAttribute("tachyon.underfs.hdfs.impl", ""
        + mTachyonConf.get(Constants.UNDERFS_HDFS_IMPL, ""));
    request.setAttribute("tachyon.underfs.glusterfs.impl", ""
        + mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_IMPL, ""));
    request.setAttribute("tachyon.underfs.glusterfs.volumes", ""
        + mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES, ""));
    request.setAttribute("tachyon.underfs.glusterfs.mounts", ""
        + mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_MOUNTS, ""));
    request.setAttribute("tachyon.underfs.glusterfs.mapred.system.dir", ""
        + mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_MR_DIR, ""));
    request.setAttribute("tachyon.web.resources", ""
        + mTachyonConf.get(Constants.WEB_RESOURCES, ""));
    request.setAttribute("tachyon.usezookeeper", ""
        + mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false));
    request.setAttribute("tachyon.zookeeper.address", ""
        + mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS, ""));
    request.setAttribute("tachyon.zookeeper.election.path", ""
        + mTachyonConf.get(Constants.ZOOKEEPER_ELECTION_PATH, ""));
    request.setAttribute("tachyon.zookeeper.leader.path", ""
        + mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH, ""));
    request.setAttribute("tachyon.async.enabled", ""
        + mTachyonConf.get(Constants.ASYNC_ENABLED, ""));
    request.setAttribute("tachyon.max.columns", ""
        + mTachyonConf.getInt(Constants.MAX_COLUMNS, 1000));
    request.setAttribute("tachyon.max.table.metadata.byte", ""
        + mTachyonConf.getBytes(Constants.MAX_TABLE_METADATA_BYTE, Constants.MB * 5));

    // MasterConf
    request.setAttribute("tachyon.master.journal.folder", ""
        + mTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER, ""));
    request.setAttribute("FORMAT_FILE_PREFIX", ""
        + mTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX, ""));
    request.setAttribute("tachyon.master.hostname", ""
        + mTachyonConf.get(Constants.MASTER_HOSTNAME, ""));
    request.setAttribute("tachyon.master.port", ""
        + mTachyonConf.getInt(Constants.MASTER_PORT, -1));
    request.setAttribute("MASTER_ADDRESS", "" + mTachyonConf.get(Constants.MASTER_ADDRESS, ""));
    request.setAttribute("tachyon.master.web.port", ""
        + mTachyonConf.getInt(Constants.MASTER_WEB_PORT, 0));
    request.setAttribute("tachyon.master.temporary.folder", ""
        + mTachyonConf.get(Constants.MASTER_TEMPORARY_FOLDER, ""));
    request.setAttribute("tachyon.underfs.hadoop.prefixes", "" 
        + mTachyonConf.get(Constants.UNDERFS_HADOOP_PREFIXS, ""));
    request.setAttribute("tachyon.test.mode", ""
        + mTachyonConf.get(Constants.IN_TEST_MODE, "false"));
    request.setAttribute("tachyon.master.retry", ""
        + mTachyonConf.getInt(Constants.MASTER_RETRY_COUNT, 29));
    request.setAttribute("tachyon.master.heartbeat.interval.ms", ""
        + mTachyonConf.getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS, -1));
    request.setAttribute("tachyon.master.min.worker.threads", ""
        + mTachyonConf.getInt(Constants.MASTER_MIN_WORKER_THREADS, -1));
    request.setAttribute("tachyon.master.max.worker.threads", ""
        + mTachyonConf.getInt(Constants.MASTER_MAX_WORKER_THREADS, -1));
    request.setAttribute("tachyon.master.worker.timeout.ms", ""
        + mTachyonConf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS, -1));

    getServletContext().getRequestDispatcher("/configuration.jsp").forward(request, response);
  }
}

