package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;
import tachyon.perf.conf.PerfConf;

/**
 * The abstract class for all the test tasks. For new test, you should create a new class which
 * extends this.
 */
public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected TaskConfiguration mTaskConf;
  protected String mTaskType;

  private PerfThread[] mThreads;

  public void initialSet(int id, String nodeName, TaskConfiguration taskConf, String taskType) {
    mId = id;
    mNodeName = nodeName;
    mTaskConf = taskConf;
    mTaskType = taskType;
  }


  public boolean cleanup(PerfTaskContext taskContext) {
    boolean ret = true;
    for (int i = 0; i < mThreads.length; i ++) {
      ret &= mThreads[i].cleanupThread(mTaskConf);
    }
    ret &= cleanupTask(taskContext);
    taskContext.setFromThread(mThreads);
    taskContext.setFinishTimeMs(System.currentTimeMillis());
    try {
      String outDirPath = PerfConf.get().OUT_FOLDER;
      File outDir = new File(outDirPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      String reportFileName =
          outDirPath + "/" + PerfConstants.PERF_CONTEXT_FILE_NAME_PREFIX + mTaskType + "-" + mId
              + "@" + mNodeName;
      taskContext.writeToFile(new File(reportFileName));
    } catch (IOException e) {
      LOG.error("Error when generate the task report", e);
      ret = false;
    }
    return ret;
  }

  /**
   * Cleanup the task. Do some following work.
   * 
   * @param taskContext the context of this task
   * @return true if cleanup successfully, false otherwise
   */
  protected abstract boolean cleanupTask(PerfTaskContext taskContext);

  /**
   * If you want to cleanup certain work directory after the whole test finished, return in here.
   * Otherwise return null.
   * 
   * @return the work directory to cleanup, otherwise null
   */
  public abstract String getCleanupDir();

  public boolean run(PerfTaskContext taskContext) {
    List<Thread> threadList = new ArrayList<Thread>(mThreads.length);
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        Thread t = new Thread(mThreads[i]);
        threadList.add(t);
      }
      for (Thread t : threadList) {
        t.start();
      }
      for (Thread t : threadList) {
        t.join();
      }
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      return false;
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      return false;
    }
    return true;
  }

  public boolean setup(PerfTaskContext taskContext) {
    taskContext.setStartTimeMs(System.currentTimeMillis());
    boolean ret = setupTask(taskContext);
    mThreads = new PerfThread[PerfConf.get().THREADS_NUM];
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        mThreads[i] = TaskType.get().getTaskThreadClass(mTaskType);
        mThreads[i].initialSet(i, mId, mNodeName, mTaskType);
        ret &= mThreads[i].setupThread(mTaskConf);
      }
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      return false;
    }
    return ret;
  }

  /**
   * Setup the task. Do some preparations.
   * 
   * @param taskContext the context of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean setupTask(PerfTaskContext taskContext);
}
