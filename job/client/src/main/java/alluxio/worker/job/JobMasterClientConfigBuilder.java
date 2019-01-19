package alluxio.worker.job;

import alluxio.conf.AlluxioConfiguration;
import alluxio.master.MasterClientConfigBuilder;
import alluxio.master.MasterInquireClient;

/**
 * A builder for instances of {@link JobMasterClientConfig}.
 */
public class JobMasterClientConfigBuilder extends MasterClientConfigBuilder {

  /**
   * Creates a builder with the given {@link AlluxioConfiguration}.
   *
   * @param alluxioConf Alluxio configuration
   */
  JobMasterClientConfigBuilder(AlluxioConfiguration alluxioConf) {
    super(alluxioConf);
  }

  /**
   * Builds the configuration, creating an instance of {@link MasterInquireClient} if none is
   * specified.
   *
   * @return a {@link JobMasterClientConfig}
   */
  @Override
  public JobMasterClientConfig build() {
    if (mMasterInquireClient == null) {
      mMasterInquireClient = MasterInquireClient.Factory.createForJobMaster(mAlluxioConf);
    }
    return new JobMasterClientConfig(mAlluxioConf, mMasterInquireClient, mSubject);
  }
}
