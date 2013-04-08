package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import tachyon.client.TachyonClient;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;

/**
 * Local Tachyon cluster for unit tests.
 */
public class LocalTachyonCluster {
  private Master mMaster = null;
  private Worker mWorker = null;

  private int mMasterPort;
  private int mWorkerPort;
  private long mWorkerCapacityBytes;

  private String mTachyonHome;
  private String mWorkerDataFolder;

  private Thread mMasterThread = null;
  private Thread mWorkerThread = null;

  private String mLocalhostName = null;

  public LocalTachyonCluster(int masterPort, int workerPort, long workerCapacityBytes) {
    mMasterPort = masterPort;
    mWorkerPort = workerPort;
    mWorkerCapacityBytes = workerCapacityBytes;
  }

  public TachyonClient getClient() {
    return TachyonClient.getClient(mLocalhostName + ":" + mMasterPort);
  }

  private void mkdir(String path) throws IOException {
    if (!(new File(path)).mkdirs()) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public void start() throws IOException {
    mTachyonHome = File.createTempFile("Tachyon", "").getAbsoluteFile() + "UnitTest";
    mWorkerDataFolder = mTachyonHome + "/ramdisk";
    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";
    String underfsFolder = mTachyonHome + "/underfs";
    mkdir(mTachyonHome);
    mkdir(mWorkerDataFolder);
    mkdir(masterDataFolder);
    mkdir(masterLogFolder);

    mLocalhostName = InetAddress.getLocalHost().getCanonicalHostName();

    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.underfs.address", underfsFolder);
    System.setProperty("tachyon.master.hostname", mLocalhostName);
    System.setProperty("tachyon.master.port", mMasterPort + "");
    System.setProperty("tachyon.master.web.port", (mMasterPort + 1) + "");
    System.setProperty("tachyon.worker.port", mWorkerPort + "");
    System.setProperty("tachyon.worker.data.port", (mWorkerPort + 1) + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    mMaster = Master.createMaster(
        new InetSocketAddress(mLocalhostName, mMasterPort), mMasterPort + 1, 1, 1, 1);

    Runnable runMaster = new Runnable() {
      public void run() {
        mMaster.start();
      }
    };
    mMasterThread = new Thread(runMaster);
    mMasterThread.start();

    CommonUtils.sleepMs(null, 10);

    mWorker = Worker.createWorker(
        new InetSocketAddress(mLocalhostName, mMasterPort), 
        new InetSocketAddress(mLocalhostName, mWorkerPort),
        mWorkerPort + 1, 1, 1, 1, mWorkerDataFolder, mWorkerCapacityBytes);
    Runnable runWorker = new Runnable() {
      public void run() {
        mWorker.start();
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();
  }

  public void stop() throws Exception {
    mMaster.stop();
    mWorker.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.master.hostname");
    System.clearProperty("tachyon.master.port");
    System.clearProperty("tachyon.master.web.port");
    System.clearProperty("tachyon.worker.port");
    System.clearProperty("tachyon.worker.data.port");
    System.clearProperty("tachyon.worker.data.folder");
    System.clearProperty("tachyon.worker.memory.size");
  }

  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(1998, 2998, 100);
    cluster.start();
    CommonUtils.sleepMs(null, 1000);
    cluster.stop();
    CommonUtils.sleepMs(null, 1000);

    cluster = new LocalTachyonCluster(1998, 2998, 100);
    cluster.start();
    CommonUtils.sleepMs(null, 1000);
    cluster.stop();
    CommonUtils.sleepMs(null, 1000);
  }
}
