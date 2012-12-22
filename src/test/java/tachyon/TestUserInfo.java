package tachyon;

import junit.framework.TestCase;

public class TestUserInfo extends TestCase {

  public void testConstructor() {
    for (int k = 1; k <= 1000; k += 50) {
      UserInfo tUserInfo = new UserInfo(k);
      assertEquals(k, tUserInfo.getUserId());
    }
  }

  public void testConstructorWithException() {
    for (int k = 0; k >= -1000; k -= 50) {
      try {
        UserInfo tUserInfo = new UserInfo(k);
        assertEquals(k, tUserInfo.getUserId());
        fail("UserId " + k + " should be invalid.");
      } catch (RuntimeException e) {
      }
    }
  }
}
