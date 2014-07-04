package tachyon.command;

import junit.framework.Assert;
import org.junit.Test;
import tachyon.Constants;

import java.io.IOException;

/**
 * Unit tests on tachyon.command.Utils.
 * 
 * Note that the test case for validatePath() is already covered in getFilePath.
 * Hence only getFilePathTest is specified.
 */
public class UtilsTest {

  @Test
  public void getFilePathTest() throws IOException {
    String[] paths =
        new String[] { Constants.HEADER + "localhost:19998/dir",
            Constants.HEADER_FT + "localhost:19998/dir", "/dir", "dir" };
    String expected = "/dir";
    for (String path : paths) {
      String result = Utils.getFilePath(path);
      Assert.assertEquals(result, expected);
    }
  }
}
