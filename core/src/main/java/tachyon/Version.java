package tachyon;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * The version of the current build.
 */
public class Version {
  public static final String VERSION;

  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

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
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    VERSION = p.getProperty("tachyon.version", "UNDEFINED");
  }

  public static void main(String[] args) {
    System.out.println("Tachyon version: " + VERSION);
  }
}
