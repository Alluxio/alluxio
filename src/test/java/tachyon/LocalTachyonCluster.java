package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import tachyon.client.TachyonFS;
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

  private List<TachyonFS> mClients = new ArrayList<TachyonFS>();

  public LocalTachyonCluster(long workerCapacityBytes) {
    mMasterPort = Constants.DEFAULT_MASTER_PORT - 1000;
    mWorkerPort = Constants.DEFAULT_WORKER_PORT - 1000;
    mWorkerCapacityBytes = workerCapacityBytes;
  }

  public LocalTachyonCluster(int masterPort, int workerPort, long workerCapacityBytes) {
    mMasterPort = masterPort;
    mWorkerPort = workerPort;
    mWorkerCapacityBytes = workerCapacityBytes;
  }

  public synchronized TachyonFS getClient() {
    mClients.add(TachyonFS.get(mLocalhostName + ":" + mMasterPort));
    return mClients.get(mClients.size() - 1);
  }

  public int getMasterPort() {
    return mMasterPort;
  }

  public int getWorkerPort() {
    return mWorkerPort;
  }

  public String getTachyonHome(){
    return mTachyonHome;
  }

  WorkerServiceHandler getWorkerServiceHandler() {
    return mWorker.getWorkerServiceHandler();    
  }

  MasterInfo getMasterInfo() {
    return mMaster.getMasterInfo();
  }

  private void mkdir(String path) throws IOException {
    if (!(new File(path)).mkdirs()) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public String getTempFolderInUnderFs() {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  public void start() throws IOException {
    mTachyonHome = File.createTempFile("Tachyon", "").getAbsoluteFile() + "UnitTest";
    mWorkerDataFolder = mTachyonHome + "/ramdisk";
    String masterJournalFolder = mTachyonHome + "/journal";
    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";
    String underfsFolder = mTachyonHome + "/underfs";
    mkdir(mTachyonHome);
    mkdir(masterJournalFolder);
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

    mkdir(CommonConf.get().UNDERFS_DATA_FOLDER);
    mkdir(CommonConf.get().UNDERFS_WORKERS_FOLDER);

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
    for (TachyonFS fs : mClients) {
      fs.close();
    }

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
    LocalTachyonCluster cluster = new LocalTachyonCluster(100);
    cluster.start();
    CommonUtils.sleepMs(null, 1000);
    cluster.stop();
    CommonUtils.sleepMs(null, 1000);

    cluster = new LocalTachyonCluster(100);
    cluster.start();
    CommonUtils.sleepMs(null, 1000);
    cluster.stop();
    CommonUtils.sleepMs(null, 1000);
  }
}
