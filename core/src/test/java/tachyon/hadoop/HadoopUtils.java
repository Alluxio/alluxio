package tachyon.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.powermock.core.classloader.MockClassLoader;
import tachyon.Constants;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public final class HadoopUtils {
  private HadoopUtils() {
  }

  public static void createByteFile(FileSystem fs, Path path, int len)
      throws IOException {
    FSDataOutputStream os = fs.create(path);
    try {
      for (int k = 0; k < len; k ++) {
        os.write((byte) k);
      }
    } finally {
      os.close();
    }
  }

  public static Configuration createConfig() {
    final Configuration conf = new Configuration();
    if (HadoopUtils.isHadoop1x()) {
      conf.set("fs." + Constants.SCHEME + ".impl", TFS.class.getName());
      conf.set("fs." + Constants.SCHEME_FT + ".impl", TFSFT.class.getName());
    }
    return conf;
  }

  public static boolean isHadoop1x() {
    return getHadoopVersion().startsWith("1");
  }

  public static boolean isHadoop2x() {
    return getHadoopVersion().startsWith("2");
  }

  public static String getHadoopVersion() {
    try {
      final URL url = getSourcePath(FileSystem.class);
      final File path = new File(url.toURI());
      final String[] splits = path.getName().split("-");
      final String last = splits[splits.length - 1];
      return last.substring(0, last.lastIndexOf("."));
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  public static URL getSourcePath(Class<?> clazz) {
    try {
      clazz = getClassLoader(clazz).loadClass(clazz.getName());
      return clazz.getProtectionDomain().getCodeSource().getLocation();
    } catch (ClassNotFoundException e) {
      throw new AssertionError("Unable to find class " + clazz.getName());
    }
  }

  public static ClassLoader getClassLoader(Class<?> clazz) {
    // Power Mock makes this hard, so try to hack it
    ClassLoader cl = clazz.getClassLoader();
    if (cl instanceof MockClassLoader) {
      cl = cl.getParent();
    }
    return cl;
  }
}
