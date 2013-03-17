package tachyon;

import org.junit.Assert;
import org.junit.Test;

public class TestUserInfo {
  @Test
  public void testConstructor() {
    for (int k = 1; k <= 1000; k += 50) {
      UserInfo tUserInfo = new UserInfo(k);
      Assert.assertEquals(k, tUserInfo.getUserId());
    }
  }

  @Test(expected = RuntimeException.class)  
  public void testConstructorWithException() {
    for (int k = 0; k >= -1000; k -= 50) {
      UserInfo tUserInfo = new UserInfo(k);
      Assert.assertEquals(k, tUserInfo.getUserId());
      Assert.fail("UserId " + k + " should be invalid.");
    }
  }
}