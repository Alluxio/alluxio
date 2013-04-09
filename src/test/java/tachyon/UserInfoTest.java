package tachyon;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.UserInfo
 */
public class UserInfoTest {
  @Test
  public void constructorTest() {
    for (int k = 1; k <= 1000; k += 50) {
      UserInfo tUserInfo = new UserInfo(k);
      Assert.assertEquals(k, tUserInfo.getUserId());
    }
  }

  @Test(expected = RuntimeException.class)  
  public void constructorWithExceptionTest() {
    for (int k = 0; k >= -1000; k -= 50) {
      UserInfo tUserInfo = new UserInfo(k);
      Assert.assertEquals(k, tUserInfo.getUserId());
      Assert.fail("UserId " + k + " should be invalid.");
    }
  }

  @Test
  public void addOwnBytesTest() {
    UserInfo tUserInfo = new UserInfo(1);
    tUserInfo.addOwnBytes(7);
    tUserInfo.addOwnBytes(70);
    tUserInfo.addOwnBytes(700);
    tUserInfo.addOwnBytes(7000);
    Assert.assertEquals(7777, tUserInfo.getOwnBytes());
  }

  @Test
  public void generalTest() {
    UserInfo tUserInfo = new UserInfo(1);
    tUserInfo.addOwnBytes(7777);
    tUserInfo.addOwnBytes(-1111);
    Assert.assertEquals(6666, tUserInfo.getOwnBytes());
  }
}