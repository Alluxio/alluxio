package tachyon.master;

import java.util.Hashtable;

import tachyon.conf.MasterConf;

public class DependencyVariables {
  public static final String MASTER_HOSTNAME = MasterConf.get().HOSTNAME;
  public static final int MASTER_PORT = MasterConf.get().PORT;

  public static Hashtable<String, String> sVariables = new Hashtable<String, String>();
}
