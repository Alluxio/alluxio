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

package alluxio.job.plan.transform.format.csv;

import static org.junit.Assert.assertEquals;

import alluxio.collections.Pair;

import org.junit.Test;

import java.math.BigInteger;

public class DecimalTest {

  @Test
  public void testBigDecimal() {
    Decimal decimal = new Decimal("decimal(10,1)");

    double v = decimal.toBigDecimal("10.555").doubleValue();

    assertEquals(10.5, v, 0.0000001);
  }

  @Test
  public void testParquetBytes() {
    Decimal decimal = new Decimal("decimal(10,1)");

    byte[] bytes = decimal.toParquetBytes("10.555");

    BigInteger bigInteger = new BigInteger(bytes);
    assertEquals(105, bigInteger.intValue());
  }

  @Test
  public void testPrecisionAndScale() {
    Pair<Integer, Integer> precisionAndScale = Decimal.getPrecisionAndScale("decimal(10   ,  2)  ");

    assertEquals((Integer) 10, precisionAndScale.getFirst());
    assertEquals((Integer) 2, precisionAndScale.getSecond());
  }
}
