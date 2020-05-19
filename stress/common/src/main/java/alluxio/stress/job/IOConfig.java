package alluxio.stress.job;

import alluxio.stress.worker.WorkerBenchParameters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Configuration for the IO throughput test to the UFS.
 */
@JsonTypeName(IOConfig.NAME)
public class IOConfig extends StressBenchConfig {
  private static final long serialVersionUID = 7883915266950426998L;
  public static final String NAME = "IO";
  // The number of streams to write to the UFS concurrently
  private int mThreadNum;
  // Size of data to write in total
  // They will be read in the read performance test
  private int mDataSize;
  // Temp dir to generate test files in
  private String mPath;
  private int mWorkerNum;

  /**
   * @param className the class name to execute
   * @param args a list of command line arguments
   * @param threadNum the number of threads to run concurrently on each worker
   * @param dataSize the size to write/read for each thread, in MB
   * @param workerNum the number of workers to run together
   * @param path the UFS path to put temporary files in
   * */
  @JsonCreator
  public IOConfig(@JsonProperty("className") String className,
                  @JsonProperty("args") List<String> args,
                  @JsonProperty("threadNum") int threadNum,
                  @JsonProperty("dataSize") int dataSize,
                  @JsonProperty("workerNum") int workerNum,
                  @JsonProperty("path") String path) {
    super(className, args, 0);
    mThreadNum = threadNum;
    mDataSize = dataSize;
    mPath = path;
    mWorkerNum = workerNum;
  }

  /**
   * @param className the class name to execute
   * @param args a list of command line arguments
   * @param params a {@link WorkerBenchParameters} to get information from
   * */
  public IOConfig(String className, List<String> args, WorkerBenchParameters params) {
    super(className, args, 0);
    mThreadNum = params.mThreads;
    mDataSize = params.mDataSize;
    mPath = params.mPath;
    mWorkerNum = params.mWorkerNum;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * @return the path to put test temporary files in
   */
  public String getPath() {
    return mPath;
  }

  /**
   * @return the data size in MB
   */
  public long getDataSize() {
    return mDataSize;
  }

  /**
   * @return the number of threads to use on each worker
   */
  public int getThreadNum() {
    return mThreadNum;
  }

  /**
   * @return the worker number
   */
  public int getWorkerNum() {
    return mWorkerNum;
  }

  @Override
  public String toString() {
    return String.format("{mThreadNum=%s, mDataSize=%s, mPath=%s, mWorkerNum=%s}",
            mThreadNum, mDataSize, mPath, mWorkerNum);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mThreadNum, mDataSize, mPath, mWorkerNum);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IOConfig)) {
      return false;
    }
    IOConfig otherConfig = (IOConfig) other;
    return otherConfig.mThreadNum == mThreadNum
            && otherConfig.mWorkerNum == mWorkerNum
            && otherConfig.mPath.equals(mPath)
            && otherConfig.mDataSize == mDataSize;
  }

  /**
   * The IOMode associated with an operation, READ or WRITE.
   * */
  public enum IOMode {
    READ,
    WRITE
  }
}
