/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authorization;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the {@link Mode} class.
 */
public final class ModeTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void defaults() {
    Mode mode = Mode.defaults();
    Assert.assertEquals(Constants.DEFAULT_FILE_SYSTEM_MODE, mode.toShort());
  }

  /**
   * Tests the {@link Mode#toShort()} method.
   */
  @Test
  public void toShort() {
    Mode mode = new Mode(Mode.Bits.ALL, Mode.Bits.READ_EXECUTE, Mode.Bits.READ_EXECUTE);
    Assert.assertEquals(0755, mode.toShort());

    mode = Mode.defaults();
    Assert.assertEquals(0777, mode.toShort());

    mode = new Mode(Mode.Bits.READ_WRITE, Mode.Bits.READ, Mode.Bits.READ);
    Assert.assertEquals(0644, mode.toShort());
  }

  /**
   * Tests the {@link Mode#fromShort(short)} method.
   */
  @Test
  public void fromShort() {
    Mode mode = new Mode((short) 0777);
    Assert.assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, mode.getOtherBits());

    mode = new Mode((short) 0644);
    Assert.assertEquals(Mode.Bits.READ_WRITE, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, mode.getOtherBits());

    mode = new Mode((short) 0755);
    Assert.assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
  }

  /**
   * Tests the {@link Mode#Mode(Mode)} constructor.
   */
  @Test
  public void copyConstructor() {
    Mode mode = new Mode(Mode.defaults());
    Assert.assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, mode.getOtherBits());
    Assert.assertEquals(0777, mode.toShort());
  }

  /**
   * Tests the {@link Mode#createNoAccess()} method.
   */
  @Test
  public void createNoAccess() {
    Mode mode = Mode.createNoAccess();
    Assert.assertEquals(Mode.Bits.NONE, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.NONE, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.NONE, mode.getOtherBits());
    Assert.assertEquals(0000, mode.toShort());
  }

  /**
   * Tests the {@link Mode#equals(Object)} method.
   */
  @Test
  public void equals() {
    Mode allAccess = new Mode((short) 0777);
    Assert.assertTrue(allAccess.equals(Mode.defaults()));
    Mode noAccess = new Mode((short) 0000);
    Assert.assertTrue(noAccess.equals(Mode.createNoAccess()));
    Assert.assertFalse(allAccess.equals(noAccess));
  }

  /**
   * Tests the {@link Mode#toString()} method.
   */
  @Test
  public void toStringTest() {
    Assert.assertEquals("rwxrwxrwx", new Mode((short) 0777).toString());
    Assert.assertEquals("rw-r-----", new Mode((short) 0640).toString());
    Assert.assertEquals("rw-------", new Mode((short) 0600).toString());
    Assert.assertEquals("---------", new Mode((short) 0000).toString());
  }

  /**
   * Tests the {@link Mode#getUMask()} and
   * {@link Mode#applyUMask(Mode)} methods.
   */
  @Test
  public void umask() {
    String umask = "0022";
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, umask);
    // after umask 0022, 0777 should change to 0755
    Mode mode = Mode.defaults().applyDirectoryUMask();
    Assert.assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
    Assert.assertEquals(0755, mode.toShort());
  }

  /**
   * Tests the {@link Mode#getUMask()} method to thrown an exception when it exceeds the length.
   */
  @Test
  public void umaskExceedLength() {
    String umask = "00022";
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, umask);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
    Mode.defaults().applyDirectoryUMask();
  }

  /**
   * Tests the {@link Mode#getUMask()} method to thrown an exception when it is not an integer.
   */
  @Test
  public void umaskNotInteger() {
    String umask = "NotInteger";
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, umask);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
    Mode.defaults().applyDirectoryUMask();
  }

  @Test
  public void setOwnerBits() {
    Mode mode = new Mode((short) 0000);
    mode.setOwnerBits(Mode.Bits.READ_EXECUTE);
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getOwnerBits());
    mode.setOwnerBits(Mode.Bits.WRITE);
    Assert.assertEquals(Mode.Bits.WRITE, mode.getOwnerBits());
    mode.setOwnerBits(Mode.Bits.ALL);
    Assert.assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
  }

  @Test
  public void setGroupBits() {
    Mode mode = new Mode((short) 0000);
    mode.setGroupBits(Mode.Bits.READ_EXECUTE);
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    mode.setGroupBits(Mode.Bits.WRITE);
    Assert.assertEquals(Mode.Bits.WRITE, mode.getGroupBits());
    mode.setGroupBits(Mode.Bits.ALL);
    Assert.assertEquals(Mode.Bits.ALL, mode.getGroupBits());
  }

  @Test
  public void setOtherBits() {
    Mode mode = new Mode((short) 0000);
    mode.setOtherBits(Mode.Bits.READ_EXECUTE);
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
    mode.setOtherBits(Mode.Bits.WRITE);
    Assert.assertEquals(Mode.Bits.WRITE, mode.getOtherBits());
    mode.setOtherBits(Mode.Bits.ALL);
    Assert.assertEquals(Mode.Bits.ALL, mode.getOtherBits());
  }
}
