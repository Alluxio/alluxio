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

package tachyon.conf;

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.NetworkType;
import tachyon.worker.allocation.AllocateStrategyType;
import tachyon.worker.eviction.EvictStrategyType;
import tachyon.worker.netty.ChannelType;
import tachyon.worker.netty.FileTransferType;

public class WorkerConf extends Utils {
  private static WorkerConf sWorkerConf = null;

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    sWorkerConf = null;
  }

  public static synchronized WorkerConf get() {
    if (sWorkerConf == null) {
      sWorkerConf = new WorkerConf();
    }

    return sWorkerConf;
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
  public static final String USER_TEMP_RELATIVE_FOLDER = "users";

  public final int WORKER_CHECKPOINT_THREADS;
  public final int WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC;

  public final NetworkType NETWORK_TYPE;
  
  public final String KEYTAB_KEY;
  public final String KEYTAB;
  public final String PRINCIPAL_KEY;
  public final String PRINCIPAL;

  public final ChannelType NETTY_CHANNEL_TYPE;
  public final FileTransferType NETTY_FILE_TRANSFER_TYPE;
  public final int NETTY_HIGH_WATER_MARK;
  public final int NETTY_BOSS_THREADS;
  public final int NETTY_WORKER_THREADS;
  public final int NETTY_LOW_WATER_MARK;
  public final Optional<Integer> NETTY_BACKLOG;
  public final Optional<Integer> NETTY_SEND_BUFFER;
  public final Optional<Integer> NETTY_RECIEVE_BUFFER;

  public final EvictStrategyType EVICT_STRATEGY_TYPE;
  public final AllocateStrategyType ALLOCATE_STRATEGY_TYPE;

  public final int STORAGE_LEVELS;
  public final StorageLevelAlias[] STORAGE_LEVEL_ALIAS;
  public final String[] STORAGE_TIER_DIRS;
  public final String[] STORAGE_TIER_DIR_QUOTA;

  private WorkerConf() {
    MASTER_HOSTNAME = getProperty("tachyon.master.hostname", NetworkUtils.getLocalHostName());
    MASTER_PORT = getIntProperty("tachyon.master.port", Constants.DEFAULT_MASTER_PORT);

    PORT = getIntProperty("tachyon.worker.port", Constants.DEFAULT_WORKER_PORT);
    DATA_PORT =
        getIntProperty("tachyon.worker.data.port", Constants.DEFAULT_WORKER_DATA_SERVER_PORT);
    DATA_FOLDER = getProperty("tachyon.worker.data.folder", "/datastore");
    MEMORY_SIZE =
        CommonUtils.parseSpaceSize(getProperty("tachyon.worker.memory.size", (128 * Constants.MB)
            + ""));
    HEARTBEAT_TIMEOUT_MS =
        getIntProperty("tachyon.worker.heartbeat.timeout.ms", 10 * Constants.SECOND_MS);
    TO_MASTER_HEARTBEAT_INTERVAL_MS =
        getIntProperty("tachyon.worker.to.master.heartbeat.interval.ms", Constants.SECOND_MS);
    SELECTOR_THREADS = getIntProperty("tachyon.worker.selector.threads", 3);
    QUEUE_SIZE_PER_SELECTOR = getIntProperty("tachyon.worker.queue.size.per.selector", 3000);
    SERVER_THREADS =
        getIntProperty("tachyon.worker.server.threads", Runtime.getRuntime().availableProcessors());
    USER_TIMEOUT_MS = getIntProperty("tachyon.worker.user.timeout.ms", 10 * Constants.SECOND_MS);

    WORKER_CHECKPOINT_THREADS = getIntProperty("tachyon.worker.checkpoint.threads", 1);
    WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC =
        getIntProperty("tachyon.worker.per.thread.checkpoint.cap.mb.sec", Constants.SECOND_MS);
    
    KEYTAB_KEY = "tachyon.worker.keytab.file";
    KEYTAB = getProperty(KEYTAB_KEY, null);
    PRINCIPAL_KEY = "tachyon.worker.principal";
    PRINCIPAL = getProperty(PRINCIPAL_KEY, null);

    NETWORK_TYPE = getEnumProperty("tachyon.worker.network.type", NetworkType.NETTY);
    NETTY_BOSS_THREADS = getIntProperty("tachyon.worker.network.netty.boss.threads", 1);
    NETTY_WORKER_THREADS = getIntProperty("tachyon.worker.network.netty.worker.threads", 0);
    NETTY_CHANNEL_TYPE =
        getEnumProperty("tachyon.worker.network.netty.channel", ChannelType.defaultType());
    NETTY_FILE_TRANSFER_TYPE =
        getEnumProperty("tachyon.worker.network.netty.file.transfer", FileTransferType.MAPPED);
    NETTY_HIGH_WATER_MARK =
        getIntProperty("tachyon.worker.network.netty.watermark.high", 32 * 1024);
    NETTY_LOW_WATER_MARK = getIntProperty("tachyon.worker.network.netty.watermark.low", 8 * 1024);
    NETTY_BACKLOG =
        Optional.fromNullable(getIntegerProperty("tachyon.worker.network.netty.backlog", null));
    NETTY_SEND_BUFFER =
        Optional.fromNullable(getIntegerProperty("tachyon.worker.network.netty.buffer.send", null));
    NETTY_RECIEVE_BUFFER =
        Optional.fromNullable(getIntegerProperty("tachyon.worker.network.netty.buffer.receive",
            null));

    EVICT_STRATEGY_TYPE = getEnumProperty("tachyon.worker.evict.strategy", EvictStrategyType.LRU);
    ALLOCATE_STRATEGY_TYPE =
        getEnumProperty("tachyon.worker.allocate.strategy", AllocateStrategyType.MAX_FREE);
    STORAGE_LEVELS = getIntProperty("tachyon.worker.hierarchystore.level.max", 1);
    STORAGE_LEVEL_ALIAS = new StorageLevelAlias[STORAGE_LEVELS];
    STORAGE_TIER_DIRS = new String[STORAGE_LEVELS];
    STORAGE_TIER_DIR_QUOTA = new String[STORAGE_LEVELS];
    for (int i = 0; i < STORAGE_LEVELS; i ++) {
      STORAGE_LEVEL_ALIAS[i] =
          getEnumProperty("tachyon.worker.hierarchystore.level" + i + ".alias",
              StorageLevelAlias.MEM);
      if (STORAGE_LEVEL_ALIAS[i].equals(StorageLevelAlias.MEM)) {
        STORAGE_TIER_DIR_QUOTA[i] =
            getProperty("tachyon.worker.hierarchystore.level" + i + ".dirs.quota",
                MEMORY_SIZE + "");
        STORAGE_TIER_DIRS[i] =
            getProperty("tachyon.worker.hierarchystore.level" + i + ".dirs.path", "/mnt/ramdisk");
      } else {
        STORAGE_TIER_DIR_QUOTA[i] =
            getProperty("tachyon.worker.hierarchystore.level" + i + ".dirs.quota");
        STORAGE_TIER_DIRS[i] =
            getProperty("tachyon.worker.hierarchystore.level" + i + ".dirs.path");
      }
    }
  }
}
