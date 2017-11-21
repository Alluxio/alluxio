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

package alluxio.wire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey.Template;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.CommonUtils;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Unit tests for {@link TieredIdentity}.
 */
public class TieredIdentityTest {

  @Test
  public void nearest() throws Exception {
    TieredIdentity id1 = TieredIdentityFactory.fromString("node=A,rack=rack1");
    TieredIdentity id2 = TieredIdentityFactory.fromString("node=B,rack=rack2");
    TieredIdentity id3 = TieredIdentityFactory.fromString("node=C,rack=rack2");
    List<TieredIdentity> identities = Arrays.asList(id1, id2, id3);

    assertSame(id1,
        TieredIdentityFactory.fromString("node=D,rack=rack1").nearest(identities).get());
    assertSame(id2,
        TieredIdentityFactory.fromString("node=B,rack=rack2").nearest(identities).get());
    assertSame(id3,
        TieredIdentityFactory.fromString("node=C,rack=rack2").nearest(identities).get());
    assertSame(id1,
        TieredIdentityFactory.fromString("host=D,rack=rack3").nearest(identities).get());
    try (Closeable c =
        new ConfigurationRule(Template.LOCALITY_TIER_STRICT.format(Constants.LOCALITY_RACK), "true")
            .toResource()) {
      assertFalse(
          TieredIdentityFactory.fromString("host=D,rack=rack3").nearest(identities).isPresent());
    }
  }

  @Test
  public void json() throws Exception {
    TieredIdentity tieredIdentity = createRandomTieredIdentity();
    ObjectMapper mapper = new ObjectMapper();
    TieredIdentity other =
        mapper.readValue(mapper.writeValueAsBytes(tieredIdentity), TieredIdentity.class);
    checkEquality(tieredIdentity, other);
  }

  @Test
  public void thrift() {
    TieredIdentity tieredIdentity = createRandomTieredIdentity();
    TieredIdentity other = TieredIdentity.fromThrift(tieredIdentity.toThrift());
    checkEquality(tieredIdentity, other);
  }

  public void checkEquality(TieredIdentity a, TieredIdentity b) {
    assertEquals(a.getTiers(), b.getTiers());
    assertEquals(a, b);
  }

  public static TieredIdentity createRandomTieredIdentity() {
    return new TieredIdentity(
        Arrays.asList(createRandomLocalityTier(), createRandomLocalityTier()));
  }

  private static LocalityTier createRandomLocalityTier() {
    Random random = new Random();

    String tier = CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    String value = CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    return new LocalityTier(tier, value);
  }
}
