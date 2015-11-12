package tachyon.client.table;

/**
 * Tachyon provides native support for tables with multiple columns. Each table contains one or more
 * columns. Each columns contains one or more ordered files.
 */
public class SimpleRawTable {
  private final long mRawTableId;

  public SimpleRawTable(long rawTableId) {
    mRawTableId = rawTableId;
  }

  public long getRawTableId() {
    return mRawTableId;
  }
}
