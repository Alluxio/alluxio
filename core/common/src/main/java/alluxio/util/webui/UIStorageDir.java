package alluxio.util.webui;

/**
 * Displays information about a storage directory in the UI.
 */
public class UIStorageDir {
  private final String mTierAlias;
  private final String mDirPath;
  private final long mCapacityBytes;
  private final long mUsedBytes;

  /**
   * Creates a new instance of {@link UIStorageDir}.
   *
   * @param tierAlias tier alias
   * @param dirPath directory path
   * @param capacityBytes capacity in bytes
   * @param usedBytes used capacity in bytes
   */
  public UIStorageDir(String tierAlias, String dirPath, long capacityBytes, long usedBytes) {
    mTierAlias = tierAlias;
    mDirPath = dirPath;
    mCapacityBytes = capacityBytes;
    mUsedBytes = usedBytes;
  }

  /**
   * @return capacity in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return directory path
   */
  public String getDirPath() {
    return mDirPath;
  }

  /**
   * @return tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * @return used capacity in bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }
}
