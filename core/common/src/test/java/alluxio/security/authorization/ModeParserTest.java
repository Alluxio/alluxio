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

import alluxio.exception.ExceptionMessage;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for the {@code ModeParser}.
 * @author rvesse
 *
 */
public final class ModeParserTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private final ModeParser mParser = new ModeParser();

  @Test
  public void numerics() {
    Mode parsed = mParser.parse("777");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = mParser.parse("755");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());

    parsed = mParser.parse("644");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }

  @Test
  public void symbolicsSeparated() {
    Mode parsed = mParser.parse("u=rwx,g=rwx,o=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = mParser.parse("u=rwx,g=rx,o=rx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());

    parsed = mParser.parse("u=rw,g=r,o=r");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }

  @Test
  public void symbolicsCombined() {
    Mode parsed = mParser.parse("a=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = mParser.parse("ugo=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = mParser.parse("u=rwx,go=rx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());

    parsed = mParser.parse("u=rw,go=r");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }

  @Test
  public void symbolicsCumulative() {
    Mode parsed = mParser.parse("u=r,u=w,u=x");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOtherBits());

    parsed = mParser.parse("g=r,g=w,g=x");
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOtherBits());

    parsed = mParser.parse("o=r,o=w,o=x");
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());
  }

  @Test
  public void symbolicsPartial() {
    Mode parsed = mParser.parse("u=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOtherBits());

    parsed = mParser.parse("go=rw");
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOtherBits());

    parsed = mParser.parse("o=x");
    Assert.assertEquals(Mode.Bits.NONE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.NONE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.EXECUTE, parsed.getOtherBits());
  }

  @Test
  public void symbolicsBadEmpty() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE.getMessage(""));
    mParser.parse("");
  }

  @Test
  public void symbolicsBadNull() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE.getMessage((Object) null));
    mParser.parse(null);
  }

  @Test
  public void symbolicsBadWhitespace() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE.getMessage("  "));
    mParser.parse("  ");
  }

  @Test
  public void symbolicsBadNoSeparator() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE_SEGMENT.getMessage("u=rwx,foo", "foo"));
    mParser.parse("u=rwx,foo");
  }

  @Test
  public void symbolicsBadTargets() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE_SEGMENT.getMessage("f=r", "f=r", "f"));
    mParser.parse("f=r");
  }

  @Test
  public void symbolicsBadPermissions() {
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.INVALID_MODE_SEGMENT
        .getMessage("u=Xst", "u=Xst", "Xst"));
    mParser.parse("u=Xst");
  }
}
