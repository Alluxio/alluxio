package tachyon;

import java.util.HashMap;
import java.util.Map;

import tachyon.conf.MasterConf;

public class DependencyVariables {
  public static final String MASTER_HOSTNAME = MasterConf.get().HOSTNAME;
  public static final int MASTER_PORT = MasterConf.get().PORT;

  public static Map<String, String> sVariables = new HashMap<String, String>();
}
