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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ModeUtils;

import org.junit.Before;
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

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @Test
  public void defaults() {
    Mode mode = Mode.defaults();
    assertEquals(Constants.DEFAULT_FILE_SYSTEM_MODE, mode.toShort());
  }

  /**
   * Tests the {@link Mode#toShort()} method.
   */
  @Test
  public void toShort() {
    Mode mode = new Mode(Mode.Bits.ALL, Mode.Bits.READ_EXECUTE, Mode.Bits.READ_EXECUTE);
    assertEquals(0755, mode.toShort());

    mode = Mode.defaults();
    assertEquals(0777, mode.toShort());

    mode = new Mode(Mode.Bits.READ_WRITE, Mode.Bits.READ, Mode.Bits.READ);
    assertEquals(0644, mode.toShort());
  }

  /**
   * Tests the {@link Mode#fromShort(short)} method.
   */
  @Test
  public void fromShort() {
    Mode mode = new Mode((short) 0777);
    assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    assertEquals(Mode.Bits.ALL, mode.getGroupBits());
    assertEquals(Mode.Bits.ALL, mode.getOtherBits());

    mode = new Mode((short) 0644);
    assertEquals(Mode.Bits.READ_WRITE, mode.getOwnerBits());
    assertEquals(Mode.Bits.READ, mode.getGroupBits());
    assertEquals(Mode.Bits.READ, mode.getOtherBits());

    mode = new Mode((short) 0755);
    assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
  }

  /**
   * Tests the {@link Mode#Mode(Mode)} constructor.
   */
  @Test
  public void copyConstructor() {
    Mode mode = new Mode(Mode.defaults());
    assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    assertEquals(Mode.Bits.ALL, mode.getGroupBits());
    assertEquals(Mode.Bits.ALL, mode.getOtherBits());
    assertEquals(0777, mode.toShort());
  }

  /**
   * Tests the {@link Mode#createNoAccess()} method.
   */
  @Test
  public void createNoAccess() {
    Mode mode = Mode.createNoAccess();
    assertEquals(Mode.Bits.NONE, mode.getOwnerBits());
    assertEquals(Mode.Bits.NONE, mode.getGroupBits());
    assertEquals(Mode.Bits.NONE, mode.getOtherBits());
    assertEquals(0000, mode.toShort());
  }

  /**
   * Tests the {@link Mode#equals(Object)} method.
   */
  @Test
  public void equals() {
    Mode allAccess = new Mode((short) 0777);
    assertTrue(allAccess.equals(Mode.defaults()));
    Mode noAccess = new Mode((short) 0000);
    assertTrue(noAccess.equals(Mode.createNoAccess()));
    assertFalse(allAccess.equals(noAccess));
  }

  /**
   * Tests the {@link Mode#toString()} method.
   */
  @Test
  public void toStringTest() {
    assertEquals("rwxrwxrwx", new Mode((short) 0777).toString());
    assertEquals("rw-r-----", new Mode((short) 0640).toString());
    assertEquals("rw-------", new Mode((short) 0600).toString());
    assertEquals("---------", new Mode((short) 0000).toString());
  }

  /**
   * Tests the {@link Mode#applyUMask(Mode)} method.
   */
  @Test
  public void applyUMask() {
    String umask = "0022";
    Mode mode = ModeUtils.applyDirectoryUMask(Mode.defaults(), umask);
    assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
    assertEquals(0755, mode.toShort());
  }

  /**
   * Tests the {@link Mode#getUMask()} method.
   */
  @Test
  public void umask() {
    assertEquals(0700, ModeUtils.getUMask("0700").toShort());
    assertEquals(0755, ModeUtils.getUMask("0755").toShort());
    assertEquals(0644, ModeUtils.getUMask("0644").toShort());
  }

  /**
   * Tests the {@link Mode#getUMask()} method to thrown an exception when it exceeds the length.
   */
  @Test
  public void umaskExceedLength() {
    String umask = "00022";
    mConfiguration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, umask);
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
    ModeUtils.applyDirectoryUMask(Mode.defaults(), mConfiguration
        .get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
  }

  /**
   * Tests the {@link Mode#getUMask()} method to thrown an exception when it is not an integer.
   */
  @Test
  public void umaskNotInteger() {
    String umask = "NotInteger";
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_CONFIGURATION_VALUE.getMessage(umask,
        PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
    ModeUtils.applyDirectoryUMask(Mode.defaults(), umask);
  }

  @Test
  public void setOwnerBits() {
    Mode mode = new Mode((short) 0000);
    mode.setOwnerBits(Mode.Bits.READ_EXECUTE);
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getOwnerBits());
    mode.setOwnerBits(Mode.Bits.WRITE);
    assertEquals(Mode.Bits.WRITE, mode.getOwnerBits());
    mode.setOwnerBits(Mode.Bits.ALL);
    assertEquals(Mode.Bits.ALL, mode.getOwnerBits());
  }

  @Test
  public void setGroupBits() {
    Mode mode = new Mode((short) 0000);
    mode.setGroupBits(Mode.Bits.READ_EXECUTE);
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getGroupBits());
    mode.setGroupBits(Mode.Bits.WRITE);
    assertEquals(Mode.Bits.WRITE, mode.getGroupBits());
    mode.setGroupBits(Mode.Bits.ALL);
    assertEquals(Mode.Bits.ALL, mode.getGroupBits());
  }

  @Test
  public void setOtherBits() {
    Mode mode = new Mode((short) 0000);
    mode.setOtherBits(Mode.Bits.READ_EXECUTE);
    assertEquals(Mode.Bits.READ_EXECUTE, mode.getOtherBits());
    mode.setOtherBits(Mode.Bits.WRITE);
    assertEquals(Mode.Bits.WRITE, mode.getOtherBits());
    mode.setOtherBits(Mode.Bits.ALL);
    assertEquals(Mode.Bits.ALL, mode.getOtherBits());
  }
}
