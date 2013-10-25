package tachyon;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * The version of the current build.
 */
public class Version {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  public static final String VERSION;

  static {
    InputStream in = null;
    Properties p = new Properties();

    try {
      in = Version.class.getClassLoader().getResourceAsStream("version.properties");
      p.load(in);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      try {
        in.close();
      } catch (Exception e){
        LOG.error(e.getMessage(), e);
      }
    }

    VERSION = p.getProperty("tachyon.version", "UNDEFINED");
  }
}
