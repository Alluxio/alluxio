package alluxio.util;


import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public final class URIUtilsTest {

  @Test
  public void hasCode() {
    // assert Exception is not Thrown
    URIUtils.hash(0, "string");
    URIUtils.hash(0, "%");
    URIUtils.hash(0, "%%");
    URIUtils.hash(0, "%%%");
    URIUtils.hash(0, "%1");
    URIUtils.hash(0, "1%");

    assertNotEquals(URIUtils.hash(0, "%"), URIUtils.hash(0, "%%"));
    assertNotEquals(URIUtils.hash(0, "%"), URIUtils.hash(0, "%1"));
    assertNotEquals(URIUtils.hash(0, "%"), URIUtils.hash(0, "%%%"));
    assertNotEquals(URIUtils.hash(0, "%"), URIUtils.hash(0, ""));
  }
}
