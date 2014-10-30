package tachyon.perf.basic;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.perf.PerfConstants;

/**
 * The abstract class for the context, which contains the statistics of the test. For new test, you
 * should create a new class which extends this.
 */
public abstract class PerfTaskContext {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected String mTaskType;

  // The basic statistics of a test.
  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess;

  public void initialSet(int id, String nodeName, String taskType, TaskConfiguration taskConf) {
    mId = id;
    mNodeName = nodeName;
    mTaskType = taskType;
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = true;
  }

  public long getFinishTimeMs() {
    return mFinishTimeMs;
  }

  public int getId() {
    return mId;
  }

  public String getNodeName() {
    return mNodeName;
  }

  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public String getTaskType() {
    return mTaskType;
  }

  /**
   * Load this task context from file.
   * 
   * @param file the input file
   * @throws IOException
   */
  public abstract void loadFromFile(File file) throws IOException;

  public void setFinishTimeMs(long finishTimeMs) {
    mFinishTimeMs = finishTimeMs;
  }

  /**
   * Set those statistics from the threads' results.
   * 
   * @param threads
   */
  public abstract void setFromThread(PerfThread[] threads);

  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  public void setSuccess(boolean success) {
    mSuccess = success;
  }

  /**
   * Output this task context to file.
   * 
   * @param file the output file
   * @throws IOException
   */
  public abstract void writeToFile(File file) throws IOException;
}
