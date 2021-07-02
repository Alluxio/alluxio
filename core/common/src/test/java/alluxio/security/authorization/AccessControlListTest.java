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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.Lists;
import com.google.common.testing.EqualsTester;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests the {@link AccessControlList} class.
 */
public class AccessControlListTest {
  private static final String OWNING_USER = "owning_user";
  private static final String OWNING_GROUP = "owning_group";
  private static final String NAMED_USER = "named_user";
  private static final String NAMED_GROUP = "named_group";
  private static final String NAMED_GROUP2 = "named_group2";
  private static final String OTHER_USER = "other_user";
  private static final String OTHER_GROUP = "other_group";

  /**
   * Tests the constructor contract.
   */
  @Test
  public void constructor() {
    AccessControlList acl = new AccessControlList();
    assertEquals("", acl.getOwningUser());
    assertEquals("", acl.getOwningGroup());
  }

  /**
   * Tests getting and setting owner and group.
   */
  @Test
  public void ownerGroup() {
    AccessControlList acl = new AccessControlList();
    acl.setOwningUser(OWNING_USER);
    acl.setOwningGroup(OWNING_GROUP);
    assertEquals(OWNING_USER, acl.getOwningUser());
    assertEquals(OWNING_GROUP, acl.getOwningGroup());
  }

  /**
   * Tests setting and getting permitted actions.
   */
  @Test
  public void actions() {
    AccessControlList acl = new AccessControlList();
    // owning user: rwx
    // owning group: -rx
    // other: ---
    // named user: rwx
    // named group: w-x
    acl.setOwningUser(OWNING_USER);
    acl.setOwningGroup(OWNING_GROUP);
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_USER).setSubject(OWNING_USER)
        .addAction(AclAction.READ).addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_GROUP).setSubject(OWNING_GROUP)
        .addAction(AclAction.READ).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OTHER).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.NAMED_USER).setSubject(NAMED_USER)
        .addAction(AclAction.READ).addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP).setSubject(NAMED_GROUP)
        .addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
    acl.updateMask();
    // Verify mode.
    // owning user
    assertTrue(checkMode(acl, OWNING_USER, Collections.emptyList(), Mode.Bits.ALL));
    // owning group
    assertTrue(checkMode(acl, OTHER_USER, Lists.newArrayList(OWNING_GROUP),
        Mode.Bits.READ_EXECUTE));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(OWNING_GROUP),
        Mode.Bits.WRITE));
    // other
    assertFalse(checkMode(acl, OTHER_USER, Collections.emptyList(), Mode.Bits.READ));
    assertFalse(checkMode(acl, OTHER_USER, Collections.emptyList(), Mode.Bits.WRITE));
    assertFalse(checkMode(acl, OTHER_USER, Collections.emptyList(), Mode.Bits.EXECUTE));
    // named user
    assertTrue(checkMode(acl, NAMED_USER, Collections.emptyList(), Mode.Bits.ALL));
    // named group
    assertTrue(checkMode(acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP),
        Mode.Bits.WRITE_EXECUTE));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP),
        Mode.Bits.READ));
  }

  private boolean checkMode(AccessControlList acl, String user, List<String> groups,
      Mode.Bits mode) {
    for (AclAction action : mode.toAclActionSet()) {
      if (!acl.checkPermission(user, groups, action)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Tests {@link AccessControlList#getMode()}.
   */
  @Test
  public void getMode() {
    AccessControlList acl = new AccessControlList();
    assertEquals(0, acl.getMode());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_USER).setSubject(OWNING_USER)
        .addAction(AclAction.READ).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_GROUP).setSubject(OWNING_GROUP)
        .addAction(AclAction.WRITE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OTHER)
        .addAction(AclAction.EXECUTE).build());
    assertEquals(new Mode(Mode.Bits.READ, Mode.Bits.WRITE, Mode.Bits.EXECUTE).toShort(),
        acl.getMode());
  }

  /**
   * Tests {@link AccessControlList#setMode(short)}.
   */
  @Test
  public void setMode() {
    AccessControlList acl = new AccessControlList();
    short mode = new Mode(Mode.Bits.EXECUTE, Mode.Bits.WRITE, Mode.Bits.READ).toShort();
    acl.setMode(mode);
    assertEquals(mode, acl.getMode());
  }

  private void setPermissions(AccessControlList acl) {
    // owning user: rwx
    // owning group: r-x
    // other: --x
    // named user: r-x
    // named group: r--
    // named group 2: -wx
    acl.setOwningUser(OWNING_USER);
    acl.setOwningGroup(OWNING_GROUP);
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_USER).setSubject(OWNING_USER)
        .addAction(AclAction.READ).addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OWNING_GROUP).setSubject(OWNING_GROUP)
        .addAction(AclAction.READ).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.OTHER)
        .addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.NAMED_USER).setSubject(NAMED_USER)
        .addAction(AclAction.READ).addAction(AclAction.EXECUTE).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP).setSubject(NAMED_GROUP)
        .addAction(AclAction.READ).build());
    acl.setEntry(new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP).setSubject(NAMED_GROUP2)
        .addAction(AclAction.WRITE).addAction(AclAction.EXECUTE).build());
    acl.updateMask();
  }

  /**
   * Tests {@link AccessControlList#checkPermission(String, List, AclAction)}.
   */
  @Test
  public void checkPermission() {
    AccessControlList acl = new AccessControlList();
    setPermissions(acl);

    assertTrue(checkMode(acl, OWNING_USER, Collections.emptyList(), Mode.Bits.ALL));

    assertTrue(checkMode(acl, NAMED_USER, Collections.emptyList(), Mode.Bits.READ_EXECUTE));
    assertFalse(checkMode(acl, NAMED_USER, Collections.emptyList(), Mode.Bits.WRITE));

    assertTrue(checkMode(acl, OTHER_USER, Lists.newArrayList(OWNING_GROUP),
        Mode.Bits.READ_EXECUTE));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(OWNING_GROUP),
        Mode.Bits.WRITE));

    assertTrue(checkMode(acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP), Mode.Bits.READ));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP),
        Mode.Bits.WRITE));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP),
        Mode.Bits.EXECUTE));

    assertTrue(checkMode(acl, OTHER_USER, Lists.newArrayList(OTHER_GROUP),
        Mode.Bits.EXECUTE));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(OTHER_GROUP),
        Mode.Bits.READ));
    assertFalse(checkMode(acl, OTHER_USER, Lists.newArrayList(OTHER_GROUP),
        Mode.Bits.WRITE));
  }

  private void assertMode(Mode.Bits expected, AccessControlList acl, String user,
      List<String> groups) {
    assertEquals(expected, acl.getPermission(user, groups).toModeBits());
  }

  /**
   * Tests {@link AccessControlList#getPermission(String, List)}.
   */
  @Test
  public void getPermission() {
    AccessControlList acl = new AccessControlList();
    setPermissions(acl);

    assertMode(Mode.Bits.ALL, acl, OWNING_USER, Collections.emptyList());
    assertMode(Mode.Bits.READ_EXECUTE, acl, NAMED_USER, Collections.emptyList());
    assertMode(Mode.Bits.READ_EXECUTE, acl, OTHER_USER, Lists.newArrayList(OWNING_GROUP));
    assertMode(Mode.Bits.READ, acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP));
    assertMode(Mode.Bits.WRITE_EXECUTE, acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP2));
    assertMode(Mode.Bits.ALL, acl, OTHER_USER, Lists.newArrayList(NAMED_GROUP, NAMED_GROUP2));
    assertMode(Mode.Bits.EXECUTE, acl, OTHER_USER, Collections.emptyList());
    assertMode(Mode.Bits.EXECUTE, acl, OTHER_USER, Lists.newArrayList(OTHER_GROUP));
  }

  /**
   * Basic equality test to make sure that subclasses are only equal to their respective types.
   */
  @Test
  public void checkEquals() {
    AccessControlList a1 = new AccessControlList();
    a1.setOwningUser("test1");
    DefaultAccessControlList a2 = new DefaultAccessControlList();
    a2.setOwningUser("test2");
    new EqualsTester()
        .addEqualityGroup(new DefaultAccessControlList(), new DefaultAccessControlList())
        .addEqualityGroup(new AccessControlList(), new AccessControlList())
        .addEqualityGroup(a1)
        .addEqualityGroup(a2)
        .testEquals();
  }
}
