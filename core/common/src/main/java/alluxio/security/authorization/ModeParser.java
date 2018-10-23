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
import alluxio.security.authorization.Mode.Bits;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Parser for {@code Mode} instances.
 * @author rvesse
 *
 */
@ThreadSafe
public final class ModeParser {

  private static final char[] VALID_TARGETS = { 'u', 'g', 'o', 'a' };
  private static final char[] VALID_PERMISSIONS = { 'r', 'w', 'x' };

  private ModeParser() {} // to prevent initialization

  /**
   * Parses the given value as a mode.
   * @param value Value
   * @return Mode
   */
  public static Mode parse(String value) {
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(ExceptionMessage.INVALID_MODE.getMessage(value));
    }

    try {
      return parseNumeric(value);
    } catch (NumberFormatException e) {
      // Treat as symbolic
      return parseSymbolic(value);
    }
  }

  private static Mode parseNumeric(String value) {
    short s = Short.parseShort(value, 8);
    return new Mode(s);
  }

  private static Mode parseSymbolic(String value) {
    Mode.Bits ownerBits = Bits.NONE;
    Mode.Bits groupBits = Bits.NONE;
    Mode.Bits otherBits = Bits.NONE;

    String[] specs = value.contains(",") ? value.split(",") : new String[] { value };
    for (String spec : specs) {
      String[] specParts = spec.split("=");

      // Validate that the spec is usable
      // Must have targets=perm i.e. 2 parts
      // Targets must be in u, g, o and a
      // Permissions must be in r, w and x
      if (specParts.length != 2) {
        throw new IllegalArgumentException(ExceptionMessage.INVALID_MODE_SEGMENT
            .getMessage(value, spec));
      }
      if (!StringUtils.containsOnly(specParts[0], VALID_TARGETS)) {
        throw new IllegalArgumentException(ExceptionMessage.INVALID_MODE_TARGETS
            .getMessage(value, spec, specParts[0]));
      }
      if (!StringUtils.containsOnly(specParts[1], VALID_PERMISSIONS)) {
        throw new IllegalArgumentException(ExceptionMessage.INVALID_MODE_PERMISSIONS
            .getMessage(value, spec, specParts[1]));
      }

      // Build the permissions being specified
      Mode.Bits specBits = Bits.NONE;
      for (char permChar : specParts[1].toCharArray()) {
        switch (permChar) {
          case 'r':
            specBits = specBits.or(Bits.READ);
            break;
          case 'w':
            specBits = specBits.or(Bits.WRITE);
            break;
          case 'x':
            specBits = specBits.or(Bits.EXECUTE);
            break;
          default:
            // Should never get here as already checked for invalid targets
            throw new IllegalStateException("Unknown permission character: " + permChar);
        }
      }

      // Apply them to the targets
      for (char targetChar : specParts[0].toCharArray()) {
        switch (targetChar) {
          case 'u':
            ownerBits = ownerBits.or(specBits);
            break;
          case 'g':
            groupBits = groupBits.or(specBits);
            break;
          case 'o':
            otherBits = otherBits.or(specBits);
            break;
          case 'a':
            ownerBits = ownerBits.or(specBits);
            groupBits = groupBits.or(specBits);
            otherBits = otherBits.or(specBits);
            break;
          default:
            // Should never get here as already checked for invalid targets
            throw new IllegalStateException("Unknown target character: " + targetChar);
        }
      }
    }

    // Return the resulting mode
    return new Mode(ownerBits, groupBits, otherBits);
  }
}
