package tachyon;

import tachyon.conf.MasterConf;

public class DependencyVariables {
  public static final String MASTER_HOSTNAME = MasterConf.get().HOSTNAME;
  public static final int MASTER_PORT = MasterConf.get().PORT;
}
