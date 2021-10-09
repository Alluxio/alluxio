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

package alluxio.stress.common;

import alluxio.stress.Parameters;

/**
 * abstract class for general Parameters used in stressBench.
 */
public abstract class GeneralParameters extends Parameters {

  /**
   * Notice the function name can't be getOperation since Jackson would transfer this function in to
   * json value, break Parameter serialization and cause serialization error.
   * @return the Operation Enum
   */
  public abstract Enum<?> operation();
}
