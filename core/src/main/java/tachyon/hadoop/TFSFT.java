package tachyon.hadoop;

import tachyon.Constants;

/**
 * An Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can work with
 * Tachyon transparently by using this class. However, it is not as efficient as using the Tachyon
 * API in tachyon.client package.
 *
 * This class will enable zookeeper.
 */
public final class TFSFT extends AbstractTFS {

  @Override
  public String getScheme() {
      return Constants.SCHEME_FT;
  }

  @Override
  protected boolean isZookeeperMode() {
    return true;
  }
}
