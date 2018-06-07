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

package alluxio.conf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests enum Source.
 */
public class SourceTest {
  @Test
  public void compareTo() {
    assertEquals(-1, Source.UNKNOWN.compareTo(Source.DEFAULT));
    assertEquals(-1, Source.DEFAULT.compareTo(Source.CLUSTER_DEFAULT));
    assertEquals(-1, Source.CLUSTER_DEFAULT.compareTo(Source.siteProperty("")));
    assertEquals(-1, Source.Type.SITE_PROPERTY.compareTo(Source.Type.SYSTEM_PROPERTY));
    assertEquals(-1, Source.SYSTEM_PROPERTY.compareTo(Source.RUNTIME));
  }
}
