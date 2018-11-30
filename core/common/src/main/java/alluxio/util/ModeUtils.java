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

package alluxio.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.security.authorization.Mode;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for mode.
 */
@ThreadSafe
public final class ModeUtils {
  private static final Mode FILE_UMASK = new Mode(Constants.FILE_DIR_PERMISSION_DIFF);

  private ModeUtils() {} // prevent instantiation

  /**
   * Applies the default umask for newly created files to this mode.
   *
   * @param mode the mode to update
   * @return the updated object
   */
  public static Mode applyFileUMask(Mode mode) {
    mode = applyUMask(mode, getUMask());
    mode = applyUMask(mode, FILE_UMASK);
    return mode;
  }

  /**
   * Applies the default umask for newly created directories to this mode.
   *
   * @param mode the mode to update
   * @return the updated object
   */
  public static Mode applyDirectoryUMask(Mode mode) {
    return applyUMask(mode, getUMask());
  }

  /**
   * Applies the given umask {@link Mode} to this mode.
   *
   * @param mode the mode to update
   * @param umask the umask to apply
   * @return the updated object
   */
  private static Mode applyUMask(Mode mode, Mode umask) {
    mode.setOwnerBits(mode.getOwnerBits().and(umask.getOwnerBits().not()));
    mode.setGroupBits(mode.getGroupBits().and(umask.getGroupBits().not()));
    mode.setOtherBits(mode.getOtherBits().and(umask.getOtherBits().not()));
    return mode;
  }

  /**
   * Gets the file / directory creation umask.
   *
   * @return the umask {@link Mode}
   */
  private static Mode getUMask() {
    int umask = Constants.DEFAULT_FILE_SYSTEM_UMASK;
    String confUmask = Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK);
    if (confUmask != null) {
      if ((confUmask.length() > 4) || !isValid(confUmask)) {
        throw new IllegalArgumentException(ExceptionMessage.INVALID_CONFIGURATION_VALUE
            .getMessage(confUmask, PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
      }
      int newUmask = 0;
      int lastIndex = confUmask.length() - 1;
      for (int i = 0; i <= lastIndex; i++) {
        newUmask += (confUmask.charAt(i) - '0') << 3 * (lastIndex - i);
      }
      umask = newUmask;
    }
    return new Mode((short) umask);
  }

  private static boolean isValid(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
