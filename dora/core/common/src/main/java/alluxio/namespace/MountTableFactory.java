package alluxio.namespace;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoraCacheClientFactory.
 */
public class MountTableFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MountTableFactory.class);

  /**
   * @param conf
   * @return instance of MountTableManager
   */
  public static MountTableManager create(AlluxioConfiguration conf) {
    String doraUfsRoot = conf.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    LOG.info("Mount table not enabled, using emulated mount table where single UFS {} is "
        + "mounted to Alluxio root", doraUfsRoot);
    return new RootOnlyMountTableManager(doraUfsRoot);
  }
}
