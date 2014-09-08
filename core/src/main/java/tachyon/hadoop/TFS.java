package tachyon.hadoop;

import tachyon.Constants;

/**
 * An Apache Hadoop FileSystem interface implementation. Any program working with Hadoop HDFS can
 * work with Tachyon transparently by using this class. However, it is not as efficient as using the
 * Tachyon API in tachyon.client package.
 */
public final class TFS extends AbstractTFS {

  @Override
  public String getScheme() {
    return Constants.SCHEME;
  }

  @Override
  protected boolean isZookeeperMode() {
    return false;
  }
}
