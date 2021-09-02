package alluxio.stress.fuse;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The summary for Fuse IO stress bench.
 */
public class FuseIOSummary implements Summary {
  private Map<String, Float> mIndividualThroughput;
  private List<String> mNodes;
  private Map<String, List<String>> mErrors;
  private FuseIOParameters mParameters;
  private BaseParameters mBaseParameters;
  private long mRecordStartMs;
  private long mEndMs;
  private long mIOBytes;
  private float mIOMBps;

  /**
   * Creates an instance.
   */
  public FuseIOSummary() {
    // Default constructor required for json deserialization
    mNodes = new ArrayList<>();
    mErrors = new HashMap<>();
    mIndividualThroughput = new HashMap<>();
  }

  /**
   * Creates an instance.
   *
   * @param parameters the parameters for the Fuse IO stress bench
   * @param baseParameters the base parameters for the Fuse IO stress bench
   * @param nodes the unique ids of all workers
   * @param errors all errors reported by workers
   * @param recordStartMs the timestamp starting counting bytes
   * @param endMs the timestamp that the test ends
   * @param ioBytes total number of bytes processed by workers
   * @param ioMBps aggregated throughput data
   * @param individualThroughput the throughput data of each worker
   */
  public FuseIOSummary(FuseIOParameters parameters, BaseParameters baseParameters,
      List<String> nodes, Map<String, List<String>> errors, long recordStartMs, long endMs,
      long ioBytes, float ioMBps, Map<String, Float> individualThroughput) {
    mIndividualThroughput = individualThroughput;
    mNodes = nodes;
    mErrors = errors;
    mParameters = parameters;
    mBaseParameters = baseParameters;
    mRecordStartMs = recordStartMs;
    mEndMs = endMs;
    mIOBytes = ioBytes;
    mIOMBps = ioMBps;
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }

  /**
   * @return a map mapping worker unique id to its throughput
   */
  public Map<String, Float> getIndividualThroughput() {
    return mIndividualThroughput;
  }

  /**
   * @param individualThroughput the map mapping worker unique id to its throughput
   */
  public void setIndividualThroughput(Map<String, Float> individualThroughput) {
    mIndividualThroughput = individualThroughput;
  }

  /**
   * @return the list of the unique ids of workers
   */
  public List<String> getNodes() {
    return mNodes;
  }

  /**
   * @param nodes the list of the unique ids of workers
   */
  public void setNodes(List<String> nodes) {
    mNodes = nodes;
  }

  /**
   * @return the list of errors
   */
  public Map<String, List<String>> getErrors() {
    return mErrors;
  }

  /**
   * @param errors  the list of errors
   */
  public void setErrors(Map<String, List<String>> errors) {
    mErrors = errors;
  }

  /**
   * @return Fuse IO stress bench parameters
   */
  public FuseIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters Fuse IO stress bench parameters
   */
  public void setParameters(FuseIOParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the base parameters
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the base parameters
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the timestamp starting counting bytes (in ms)
   */
  public long getRecordStartMs() {
    return mRecordStartMs;
  }

  /**
   * @param recordStartMs the timestamp starting counting bytes (in ms)
   */
  public void setRecordStartMs(long recordStartMs) {
    mRecordStartMs = recordStartMs;
  }

  /**
   * @return the timestamp that test ends (in ms)
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @param endMs the timestamp that test ends (in ms)
   */
  public void setEndMs(long endMs) {
    mEndMs = endMs;
  }

  /**
   * @return total number of bytes processed during test time
   */
  public long getIOBytes() {
    return mIOBytes;
  }

  /**
   * @param ioBytes total number of bytes processed during test time
   */
  public void setIOBytes(long ioBytes) {
    mIOBytes = ioBytes;
  }

  /**
   * @return overall throughput (in MB / s)
   */
  public float getIOMBps() {
    return mIOMBps;
  }

  /**
   * @param ioMBps overall throughput (in MB / s)
   */
  public void setIOMBps(float ioMBps) {
    mIOMBps = ioMBps;
  }
}
