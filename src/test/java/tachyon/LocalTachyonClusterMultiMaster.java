package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.test.TestingServer;

import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;

/**
 * A local Tachyon cluster with Multiple masters 
 */
public class LocalTachyonClusterMultiMaster {
  private TestingServer mCuratorServer = null;

  private int mNumOfMasters = 0;
  private List<Master> mMasters = null;
  private Worker mWorker = null;

  private List<Integer> mMastersPorts;
  private int mWorkerPort;
  private long mWorkerCapacityBytes;

  private String mTachyonHome;
  private String mWorkerDataFolder;

  private List<MasterThread> mMasterThreads = null;
  private Thread mWorkerThread = null;

  private String mLocalhostName = null;

  private List<TachyonFS> mClients = new ArrayList<TachyonFS>();

  public LocalTachyonClusterMultiMaster(long workerCapacityBytes, int masters) {
    this(Constants.DEFAULT_MASTER_PORT - 1000, Constants.DEFAULT_WORKER_PORT - 1000,
        workerCapacityBytes, masters);
  }

  public LocalTachyonClusterMultiMaster(int masterPort, int workerPort, long workerCapacityBytes,
      int masters) {
    mNumOfMasters = masters;
    mMastersPorts = new ArrayList<Integer>(masters);
    for (int k = 0; k < mNumOfMasters; k ++) {
      mMastersPorts.add(masterPort + k * 10);
    }
    mWorkerPort = workerPort;
    mWorkerCapacityBytes = workerCapacityBytes;

    try {
      mCuratorServer = new TestingServer();
    } catch (Exception e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized TachyonFS getClient() {
    mClients.add(TachyonFS.get(Constants.FT_HEADER + mCuratorServer.getConnectString()));
    return mClients.get(mClients.size() - 1);
  }

  public List<Integer> getMastersPorts() {
    return mMastersPorts;
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

  MasterInfo getMasterInfo(int masterIndex) {
    return mMasters.get(masterIndex).getMasterInfo();
  }

  String getEditLogPath() {
    return mTachyonHome + "/journal/log.data";
  }

  String getImagePath() {
    return mTachyonHome + "/journal/image.data";
  }

  private void mkdir(String path) throws IOException {
    if (!(new File(path)).mkdirs()) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public String getTempFolderInUnderFs() {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  public class MasterThread extends Thread {
    private Master mMaster = null;

    public MasterThread(Master master) {
      mMaster = master;
    }

    public void run() {
      mMaster.start();
    }

    public void shutdown() {
      try {
        mMaster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
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
    System.setProperty("tachyon.usezookeeper", "true");
    System.setProperty("tachyon.zookeeper.address", mCuratorServer.getConnectString());
    System.setProperty("tachyon.zookeeper.election.path", "/election");
    System.setProperty("tachyon.zookeeper.leader.path", "/leader");
    System.setProperty("tachyon.master.hostname", mLocalhostName);
    System.setProperty("tachyon.master.port", mMastersPorts.get(0) + "");
    System.setProperty("tachyon.master.web.port", (mMastersPorts.get(0) + 1) + "");
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

    mMasters = new ArrayList<Master>();
    mMasterThreads = new ArrayList<MasterThread>();
    for (int k = 0; k < mNumOfMasters; k ++) {
      System.out.println(0);
      mMasters.add(new Master(new InetSocketAddress(mLocalhostName, mMastersPorts.get(k)),
          mMastersPorts.get(k) + 1, 1, 1, 1));
      System.out.println(1);
      MasterThread thread = new MasterThread(mMasters.get(k));
      System.out.println(2);
      thread.start();
      System.out.println(3);
      mMasterThreads.add(thread);
      System.out.println(4);
    }

    CommonUtils.sleepMs(null, 10);
    System.out.println(5);

    mWorker = Worker.createWorker(
        CommonUtils.parseInetSocketAddress(mCuratorServer.getConnectString()), 
        new InetSocketAddress(mLocalhostName, mWorkerPort),
        mWorkerPort + 1, 1, 1, 1, mWorkerDataFolder, mWorkerCapacityBytes);
    System.out.println(6);
    Runnable runWorker = new Runnable() {
      public void run() {
        mWorker.start();
      }
    };
    System.out.println(7);
    mWorkerThread = new Thread(runWorker);
    System.out.println(8);
    mWorkerThread.start();
    System.out.println(9);
  }

  public void stop() throws Exception {
    System.out.println("LTC Stop " + 1);
    for (TachyonFS fs : mClients) {
      fs.close();
    }

    System.out.println("LTC Stop " + 2);

    for (int k = 0; k < mNumOfMasters; k ++) {
      mMasterThreads.get(k).shutdown();
    }
    System.out.println("LTC Stop " + 3);
    mWorker.stop();
    System.out.println("LTC Stop " + 4);

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.underfs.address");
    System.clearProperty("tachyon.usezookeeper");
    System.clearProperty("tachyon.zookeeper.address");
    System.clearProperty("tachyon.zookeeper.election.path");
    System.clearProperty("tachyon.zookeeper.leader.path");
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
