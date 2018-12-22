package alluxio.util.webui;

import alluxio.util.CommonUtils;

/**
 * Displays information about a worker in the UI.
 */
public class UIWorkerInfo {
  private final String mWorkerAddress;
  private final long mStartTimeMs;

  /**
   * Creates a new instance of {@link UIWorkerInfo}.
   *
   * @param workerAddress worker address
   * @param startTimeMs start time in milliseconds
   */
  public UIWorkerInfo(String workerAddress, long startTimeMs) {
    mWorkerAddress = workerAddress;
    mStartTimeMs = startTimeMs;
  }

  /**
   * @return the start time
   */
  public String getStartTime() {
    return CommonUtils.convertMsToDate(mStartTimeMs);
  }

  /**
   * @return the uptime
   */
  public String getUptime() {
    return CommonUtils.convertMsToClockTime(System.currentTimeMillis() - mStartTimeMs);
  }

  /**
   * @return the worker address
   */
  public String getWorkerAddress() {
    return mWorkerAddress;
  }
}
