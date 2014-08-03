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

import tachyon.Constants;
import tachyon.util.CommonUtils;

public class WorkerConf extends Utils {
  private static WorkerConf WORKER_CONF = null;

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    WORKER_CONF = null;
  }

  public static synchronized WorkerConf get() {
    if (WORKER_CONF == null) {
      WORKER_CONF = new WorkerConf();
    }

    return WORKER_CONF;
  }

  public final String MASTER_HOSTNAME;
  public final int MASTER_PORT;
  public final int PORT;
  public final int DATA_PORT;
  public final String DATA_FOLDER;
  public final long MEMORY_SIZE;
  public final long HEARTBEAT_TIMEOUT_MS;
  public final int TO_MASTER_HEARTBEAT_INTERVAL_MS;
  public final int SELECTOR_THREADS;
  public final int QUEUE_SIZE_PER_SELECTOR;
  public final int SERVER_THREADS;

  public final int USER_TIMEOUT_MS;
  public final String USER_TEMP_RELATIVE_FOLDER = "users";

  public final int WORKER_CHECKPOINT_THREADS;

  public final int WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC;

  private WorkerConf() {
    MASTER_HOSTNAME = getProperty("tachyon.master.hostname", "localhost");
    MASTER_PORT = getIntProperty("tachyon.master.port", Constants.DEFAULT_MASTER_PORT);

    PORT = getIntProperty("tachyon.worker.port", Constants.DEFAULT_WORKER_PORT);
    DATA_PORT =
        getIntProperty("tachyon.worker.data.port", Constants.DEFAULT_WORKER_DATA_SERVER_PORT);
    DATA_FOLDER = getProperty("tachyon.worker.data.folder", "/mnt/ramdisk");
    MEMORY_SIZE =
        CommonUtils.parseSpaceSize(getProperty("tachyon.worker.memory.size", (128 * Constants.MB)
            + ""));
    HEARTBEAT_TIMEOUT_MS =
        getIntProperty("tachyon.worker.heartbeat.timeout.ms", 10 * Constants.SECOND_MS);
    TO_MASTER_HEARTBEAT_INTERVAL_MS =
        getIntProperty("tachyon.worker.to.master.heartbeat.interval.ms", Constants.SECOND_MS);
    SELECTOR_THREADS = getIntProperty("tachyon.worker.selector.threads", 3);
    QUEUE_SIZE_PER_SELECTOR = getIntProperty("tachyon.worker.queue.size.per.selector", 3000);
    SERVER_THREADS = getIntProperty("tachyon.worker.server.threads", 128);
    USER_TIMEOUT_MS = getIntProperty("tachyon.worker.user.timeout.ms", 10 * Constants.SECOND_MS);

    WORKER_CHECKPOINT_THREADS = getIntProperty("tachyon.worker.checkpoint.threads", 1);
    WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC =
        getIntProperty("tachyon.worker.per.thread.checkpoint.cap.mb.sec", Constants.SECOND_MS);
  }
}
