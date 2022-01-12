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

import org.slf4j.Logger;

/**
 * Mask some sensitive information.
 */
public interface SensitiveConfigMask {

  /**
   * Copy and mask objects' sensitive information.
   * @param logger log writer
   * @param args the objects to be masked
   * @return an array of objects masked
   */
  Object[] maskObjects(Logger logger, Object... args);
}
