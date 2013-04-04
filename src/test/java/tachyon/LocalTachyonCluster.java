package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

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

  public LocalTachyonCluster(int masterPort, int workerPort, long workerCapacityBytes) {
    mMasterPort = masterPort;
    mWorkerPort = workerPort;
    mWorkerCapacityBytes = workerCapacityBytes;
  }

  private void mkdir(String path) throws IOException {
    if (!(new File(path)).mkdirs()) {
      throw new IOException("Failed to make folder: " + path);
    }
    System.out.println("Made folder " + path);
  }

  public void start() throws IOException {
    mTachyonHome = File.createTempFile("Tachyon", "").getAbsoluteFile() + "UnitTest";
    mWorkerDataFolder = mTachyonHome + "/ramdisk";
    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";
    mkdir(mTachyonHome);
    mkdir(mWorkerDataFolder);
    mkdir(masterDataFolder);
    mkdir(masterLogFolder);

    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.master.hostname", "localhost");
    System.setProperty("tachyon.master.port", mMasterPort + "");
    System.setProperty("tachyon.master.web.port", (mMasterPort + 1) + "");
    System.setProperty("tachyon.worker.port", mWorkerPort + "");
    System.setProperty("tachyon.worker.data.port", (mWorkerPort + 1) + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");

    mMaster = Master.createMaster(
        new InetSocketAddress("localhost", mMasterPort), mMasterPort + 1, 1, 1, 1);
    
    Runnable runMaster = new Runnable() {
      public void run() {
        mMaster.start();
      }
    };
    mMasterThread = new Thread(runMaster);
    mMasterThread.start();

    CommonUtils.sleepMs(null, 10 * 1000);

    mWorker = Worker.createWorker(
        new InetSocketAddress("localhost", mMasterPort), 
        new InetSocketAddress("localhost", mWorkerPort),
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
  }

  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(1998, 2998, 100);
    cluster.start();
    CommonUtils.sleepMs(null, 10 * 1000);
    cluster.stop();

    System.out.println("I'm done.");
  }
}
