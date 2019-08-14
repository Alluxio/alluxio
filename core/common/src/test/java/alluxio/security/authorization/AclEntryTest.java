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
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Tests {@link AclEntry} class.
 */
public class AclEntryTest {

  @Test
  public void cliStringOwner() {
    checkCliString("user::---");
    checkCliString("user::--x");
    checkCliString("user::-w-");
    checkCliString("user::-wx");
    checkCliString("user::r--");
    checkCliString("user::r-x");
    checkCliString("user::rw-");
    checkCliString("user::rwx");
  }

  @Test
  public void cliStringNamedOwner() {
    checkCliString("user:a:---");
    checkCliString("user:b:r--");
    checkCliString("user:c:-w-");
    checkCliString("user:d:--x");
  }

  @Test
  public void cliStringOwningGroup() {
    checkCliString("group::---");
    checkCliString("group::r--");
    checkCliString("group::-w-");
    checkCliString("group::--x");
  }

  @Test
  public void cliStringNamedGroup() {
    checkCliString("group:a:---");
    checkCliString("group:b:r--");
    checkCliString("group:c:-w-");
    checkCliString("group:d:--x");
  }

  @Test
  public void cliStringMask() {
    checkCliString("mask::---");
    checkCliString("mask::r--");
    checkCliString("mask::-w-");
    checkCliString("mask::--x");
  }

  @Test
  public void cliStringOther() {
    checkCliString("other::---");
    checkCliString("other::r--");
    checkCliString("other::-w-");
    checkCliString("other::--x");
  }

  @Test
  public void cliStringDefault() {
    checkCliString("default:user::rwx");
    checkCliString("default:user:a:rwx");
    checkCliString("default:group::r--");
    checkCliString("default:group:b:-w-");
    checkCliString("default:mask::--x");
    checkCliString("default:other::---");
  }

  @Test
  public void cliStringInvalid() {
    checkCliStringInvalid(null);
    checkCliStringInvalid("");
    checkCliStringInvalid("::");
    checkCliStringInvalid("bad");
    checkCliStringInvalid("bad:bad:bad");

    checkCliStringInvalid("user::----");
    checkCliStringInvalid("user::r");
    checkCliStringInvalid("user::rrr");

    checkCliStringInvalid("group::----");
    checkCliStringInvalid("group::w");
    checkCliStringInvalid("group::www");

    checkCliStringInvalid("mask:mask:---");
    checkCliStringInvalid("other:other:---");

    checkCliStringInvalid("default:user:test");
  }

  private void checkCliString(String stringEntry) {
    AclEntry entry = AclEntry.fromCliString(stringEntry);
    String toString = entry.toCliString();
    assertEquals(stringEntry, toString);
  }

  private void checkCliStringInvalid(String stringEntry) {
    try {
      checkCliString(stringEntry);
      fail("this is expected to fail");
    } catch (IllegalArgumentException e) {
      // expected
    } catch (Exception e) {
      // unexpected
      fail("Unexpected exception");
    }
  }
}
