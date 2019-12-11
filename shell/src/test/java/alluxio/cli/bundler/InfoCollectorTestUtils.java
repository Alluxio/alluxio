package alluxio.cli.bundler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class InfoCollectorTestUtils {
  public static File createFileInDir(File dir, String fileName) throws IOException {
    File newFile = new File(Paths.get(dir.getAbsolutePath(), fileName).toString());
    newFile.createNewFile();
    return newFile;
  }
}
