package tachyon.master;

import java.util.Hashtable;

import tachyon.conf.MasterConf;

/**
 * The variables used to generate the command of the Dependency.
 */
public class DependencyVariables {
  public static final String MASTER_HOSTNAME = MasterConf.get().HOSTNAME;
  public static final int MASTER_PORT = MasterConf.get().PORT;

  public static final Hashtable<String, String> sVariables = new Hashtable<String, String>();
}
