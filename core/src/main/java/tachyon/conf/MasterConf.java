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

import java.util.ArrayList;
import java.util.Arrays;

import tachyon.Constants;

/**
 * Configurations used by master only.
 */
public class MasterConf extends Utils {
  private static MasterConf MASTER_CONF = null;

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    MASTER_CONF = null;
  }

  public static synchronized MasterConf get() {
    if (MASTER_CONF == null) {
      MASTER_CONF = new MasterConf();
    }

    return MASTER_CONF;
  }

  public final String JOURNAL_FOLDER;
  public final String FORMAT_FILE_PREFIX;
  public final String HOSTNAME;
  public final int PORT;
  public final String MASTER_ADDRESS;

  public final int WEB_PORT;
  public final String TEMPORARY_FOLDER;
  public final int HEARTBEAT_INTERVAL_MS;
  public final int SELECTOR_THREADS;
  public final int QUEUE_SIZE_PER_SELECTOR;

  public final int SERVER_THREADS;
  public final int WORKER_TIMEOUT_MS;

  public final ArrayList<String> WHITELIST = new ArrayList<String>();

  private MasterConf() {
    String journalFolder =
        getProperty("tachyon.master.journal.folder", CommonConf.get().TACHYON_HOME + "/journal/");
    if (!journalFolder.endsWith(Constants.PATH_SEPARATOR)) {
      journalFolder += Constants.PATH_SEPARATOR;
    }
    JOURNAL_FOLDER = journalFolder;
    FORMAT_FILE_PREFIX = "_format_";

    HOSTNAME = getProperty("tachyon.master.hostname", "localhost");
    PORT = getIntProperty("tachyon.master.port", Constants.DEFAULT_MASTER_PORT);
    MASTER_ADDRESS =
        (CommonConf.get().USE_ZOOKEEPER ? Constants.HEADER_FT : Constants.HEADER) + HOSTNAME + ":"
            + PORT;
    WEB_PORT = getIntProperty("tachyon.master.web.port", Constants.DEFAULT_MASTER_WEB_PORT);
    TEMPORARY_FOLDER = getProperty("tachyon.master.temporary.folder", "/tmp");

    HEARTBEAT_INTERVAL_MS = getIntProperty("tachyon.master.heartbeat.interval.ms", Constants.SECOND_MS);
    SELECTOR_THREADS = getIntProperty("tachyon.master.selector.threads", 3);
    QUEUE_SIZE_PER_SELECTOR = getIntProperty("tachyon.master.queue.size.per.selector", 3000);
    SERVER_THREADS = getIntProperty("tachyon.master.server.threads", 128);
    WORKER_TIMEOUT_MS = getIntProperty("tachyon.master.worker.timeout.ms", 10 * Constants.SECOND_MS);

    WHITELIST.addAll(Arrays.asList(getProperty("tachyon.master.whitelist",
        Constants.PATH_SEPARATOR).split(",")));
    String tPinList = getProperty("tachyon.master.pinlist", null);
    if (tPinList != null && !tPinList.isEmpty()) {
      System.err.println("WARNING: tachyon.master.pinlist is set but no longer supported!"
          + " Please use the pin function in the TFS Shell instead.");
    }
  }
}
