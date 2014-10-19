package tachyon.conf;

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.NetworkType;
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
  public final String USER_TEMP_RELATIVE_FOLDER = "users";

  public final int WORKER_CHECKPOINT_THREADS;
  public final int WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC;

  public final NetworkType NETWORK_TYPE;

  public final ChannelType NETTY_CHANNEL_TYPE;
  public final FileTransferType NETTY_FILE_TRANSFER_TYPE;
  public final int NETTY_HIGH_WATER_MARK;
  public final int NETTY_BOSS_THREADS;
  public final int NETTY_WORKER_THREADS;
  public final int NETTY_LOW_WATER_MARK;
  public final Optional<Integer> NETTY_BACKLOG;
  public final Optional<Integer> NETTY_SEND_BUFFER;
  public final Optional<Integer> NETTY_RECIEVE_BUFFER;

  private WorkerConf() {
    MASTER_HOSTNAME = getProperty("tachyon.master.hostname", NetworkUtils.getLocalHostName());
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
    SERVER_THREADS =
        getIntProperty("tachyon.worker.server.threads", Runtime.getRuntime().availableProcessors());
    USER_TIMEOUT_MS = getIntProperty("tachyon.worker.user.timeout.ms", 10 * Constants.SECOND_MS);

    WORKER_CHECKPOINT_THREADS = getIntProperty("tachyon.worker.checkpoint.threads", 1);
    WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC =
        getIntProperty("tachyon.worker.per.thread.checkpoint.cap.mb.sec", Constants.SECOND_MS);

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
        Optional.fromNullable(getIntegerProperty("tachyon.worker.network.netty.buffer.recieve",
            null));
  }
}
