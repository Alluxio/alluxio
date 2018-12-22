package alluxio.util.webui;

/**
 * A wrapper class of the usage info per tier for displaying in the UI.
 * This is mainly used to avoid using Map in jsp, which could cause problem with Java 8.
 * See https://alluxio.atlassian.net/browse/ALLUXIO-22.
 */
public class UIUsageOnTier {
  private final String mTierAlias;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Creates a new instance of {@link UIUsageOnTier}.
   *
   * @param tierAlias tier alias
   * @param capacityBytes capacity in bytes
   * @param usedBytes used space in bytes
   */
  public UIUsageOnTier(String tierAlias, long capacityBytes, long usedBytes) {
    mTierAlias = tierAlias;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * @return the tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * @return capacity in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return used space in bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }
}
