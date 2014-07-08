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
package tachyon.web;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.master.MasterInfo;

import java.io.IOException;

/**
 * Servlet that provides data for displaying the system's configuration.
 */
public class WebInterfaceConfigurationServlet extends HttpServlet {
  private static final long serialVersionUID = 2134205675393443914L;

  private MasterInfo mMasterInfo;
  private CommonConf mCommonConf;
  private MasterConf mMasterConf;

  public WebInterfaceConfigurationServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
    mCommonConf = CommonConf.get();
    mMasterConf = MasterConf.get();
  }

  /**
   * Populates attributes before redirecting to a jsp.
   * 
   * @param request
   *          The HttpServletRequest object
   * @param response
   *          The HttpServletReponse object
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("whitelist", mMasterInfo.getWhiteList());

    //CommonConf
    request.setAttribute("tachyon.home", "" + mCommonConf.TACHYON_HOME);
    request.setAttribute("tachyon.underfs.address", "" + mCommonConf.UNDERFS_ADDRESS);
    request.setAttribute("tachyon.data.folder", "" + mCommonConf.UNDERFS_DATA_FOLDER);
    request.setAttribute("tachyon.workers.folder", "" + mCommonConf.UNDERFS_WORKERS_FOLDER);
    request.setAttribute("tachyon.underfs.hdfs.impl", "" + mCommonConf.UNDERFS_HDFS_IMPL);
    request.setAttribute("tachyon.underfs.glusterfs.impl", "" + mCommonConf.UNDERFS_GLUSTERFS_IMPL);
    request.setAttribute("tachyon.underfs.glusterfs.volumes", "" + mCommonConf.UNDERFS_GLUSTERFS_VOLUMES);
    request.setAttribute("tachyon.underfs.glusterfs.mounts", "" + mCommonConf.UNDERFS_GLUSTERFS_MOUNTS);
    request.setAttribute("tachyon.underfs.glusterfs.mapred.system.dir", "" + mCommonConf.UNDERFS_GLUSTERFS_MR_DIR);
    request.setAttribute("tachyon.web.resources", "" + mCommonConf.WEB_RESOURCES);
    request.setAttribute("tachyon.usezookeeper", "" + mCommonConf.USE_ZOOKEEPER);
    request.setAttribute("tachyon.zookeeper.address", "" + mCommonConf.ZOOKEEPER_ADDRESS);
    request.setAttribute("tachyon.zookeeper.election.path", "" + mCommonConf.ZOOKEEPER_ELECTION_PATH);
    request.setAttribute("tachyon.zookeeper.leader.path", "" + mCommonConf.ZOOKEEPER_LEADER_PATH);
    request.setAttribute("tachyon.async.enabled", "" + mCommonConf.ASYNC_ENABLED);
    request.setAttribute("tachyon.max.columns", "" + mCommonConf.MAX_COLUMNS);
    request.setAttribute("tachyon.max.table.metadata.byte", "" + mCommonConf.MAX_TABLE_METADATA_BYTE);

    //MasterConf
    request.setAttribute("tachyon.master.journal.folder", "" + mMasterConf.JOURNAL_FOLDER);
    request.setAttribute("FORMAT_FILE_PREFIX", "" + mMasterConf.FORMAT_FILE_PREFIX);
    request.setAttribute("tachyon.master.hostname", "" + mMasterConf.HOSTNAME);
    request.setAttribute("tachyon.master.port", "" + mMasterConf.PORT);
    request.setAttribute("MASTER_ADDRESS", "" + mMasterConf.MASTER_ADDRESS);
    request.setAttribute("tachyon.master.web.port", "" + mMasterConf.WEB_PORT);
    request.setAttribute("tachyon.master.temporary.folder", "" + mMasterConf.TEMPORARY_FOLDER);
    request.setAttribute("tachyon.master.heartbeat.interval.ms", "" + mMasterConf.HEARTBEAT_INTERVAL_MS);
    request.setAttribute("tachyon.master.selector.threads", "" + mMasterConf.SELECTOR_THREADS);
    request.setAttribute("tachyon.master.queue.size.per.selector", "" + mMasterConf.QUEUE_SIZE_PER_SELECTOR);
    request.setAttribute("tachyon.master.server.threads", "" + mMasterConf.SERVER_THREADS);
    request.setAttribute("tachyon.master.worker.timeout.ms", "" + mMasterConf.WORKER_TIMEOUT_MS);

    getServletContext().getRequestDispatcher("/configuration.jsp").forward(request, response);
  }
}