package alluxio.csi.client;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to pass around Csi client options.
 */
@ThreadSafe
public final class CsiClientOptions {
  private final String mOperation;
  private final String mTarget;
  private final String mVolumeId;

  /**
   * @param operation the csi operation
   * @param target the target path
   * @param volumeId the volume id
   */
  public CsiClientOptions(String operation, String target, String volumeId) {
    mOperation = operation;
    mTarget = target;
    mVolumeId = volumeId;
  }

  /**
   * @return the csi operation
   */
  public String getOperation() {
    return mOperation;
  }

  /**
   * @return the target path
   */
  public String getTarget() {
    return mTarget;
  }

  /**
   * @return the volume id
   */
  public String getVolumeId() {
    return mVolumeId;
  }
}
