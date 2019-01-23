package alluxio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import com.sun.net.httpserver.HttpPrincipal;
import org.junit.Test;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

public class ClientContextTest {

  @Test
  public void hashCodeTest() {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.MASTER_RPC_PORT, "10293");
    HashSet<Principal> principals = new HashSet<>();
    HttpPrincipal testPrincipal = new HttpPrincipal("test", "test");
    principals.add(testPrincipal);
    Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
    ClientContext ctx1 = ClientContext.create(conf);
    ClientContext ctx2 = ClientContext.create(conf);
    ClientContext ctx3 = ClientContext.create(ConfigurationTestUtils.defaults());
    ClientContext ctx4 = ClientContext.create(ConfigurationTestUtils.defaults());
    ClientContext ctx5 = ClientContext.create(new Subject(), null);
    ClientContext ctx6 = ClientContext.create(new Subject(), null);
    ClientContext ctx7 = ClientContext.create(subject, null);
    ClientContext ctx8 = ClientContext.create(subject, null);
    ClientContext ctx9 = ClientContext.create(subject, conf);
    ClientContext ctx10 = ClientContext.create(subject, conf);
    new HashCodeTester()
        .addHashGroup(ctx1, ctx2)
        .addHashGroup(ctx3, ctx4, ctx5, ctx6, ClientContext.create())
        .addHashGroup(ctx7, ctx8)
        .addHashGroup(ctx9, ctx10)
        .testHash();
  }

  class HashCodeTester {

    HashMap<Integer, List<Object>> mGroups = new HashMap<>();
    private int mCtr = 0;

    HashCodeTester addHashGroup(Object ...items) {
      mGroups.put(mCtr, Arrays.asList(items));
      mCtr++;
      return this;
    }

    void testHash() {
      for (Map.Entry<Integer, List<Object>> group: mGroups.entrySet()) {
        int itemNum = 0;
        int hashCode = 0;
        boolean init = false;
        for (Object item : group.getValue()) {
          if (!init) {
            hashCode = item.hashCode();
            init = true;
            itemNum++;
            continue;
          }
          assertEquals(String.format("[group, item] Hash code [%d %d] does not match [%d, %d]",
              group.getKey(), itemNum, group.getKey(), itemNum), item.hashCode(), hashCode);
          for (Map.Entry<Integer, List<Object>> otherGroup : mGroups.entrySet()) {
            if (group.getKey().equals(otherGroup.getKey())) {
              continue;
            }
            int itemCheckNum = 0;
            for (Object checkItem : otherGroup.getValue()) {
              assertNotEquals(String.format("[group, item] [%d, %d] matches the hash in [%d, %d]",
                  group.getKey(), itemNum, otherGroup.getKey(), itemCheckNum), item.hashCode(),
                  checkItem.hashCode());
              itemCheckNum++;
            }
          }
          itemNum++;
        }
      }
    }
  }
}
