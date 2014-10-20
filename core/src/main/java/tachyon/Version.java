package tachyon;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The version of the current build.
 */
public class Version {
  public static final String VERSION;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  static {
    InputStream in = null;
    Properties p = new Properties();

    try {
      in = Version.class.getClassLoader().getResourceAsStream("version.properties");
      p.load(in);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    VERSION = p.getProperty("tachyon.version", "UNDEFINED");
  }

  public static void main(String[] args) {
    System.out.println("Tachyon version: " + VERSION);
  }
}
