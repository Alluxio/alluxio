package tachyon.command;

import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit tests on tachyon.command.Utils.
 *
 * Note that the test case for validatePath() is already covered in getFilePath.
 * Hence only getFilePathTest is specified.
 */
public class TFsShellUtilsTest {

  @Test
  public void getFilePathTest() throws IOException {
    String[] paths = new String[] {
        "tachyon://localhost:19998/dir",
        "tachyon-ft://localhost:19998/dir",
        "/dir",
        "dir"
    };
    String expected = "/dir";
    for (String p: paths) {
      String result = Utils.getFilePath(p);
      Assert.assertEquals(result, expected);
    }
  }

}
