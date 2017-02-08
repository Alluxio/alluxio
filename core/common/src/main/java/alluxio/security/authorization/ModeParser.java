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

import alluxio.security.authorization.Mode.Bits;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/**
 * Parser for {@code Mode} instances.
 * @author rvesse
 *
 */
public final class ModeParser {

  /**
   * Creates a new parser.
   */
  public ModeParser() {}

  /**
   * Parses the given value as a mode.
   * @param value Value
   * @return Mode
   */
  public Mode parse(String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "Invalid mode %s", value);

    try {
      return parseNumeric(value);
    } catch (NumberFormatException e) {
      // Treat as symbolic
      return parseSymbolic(value);
    }
  }

  private Mode parseNumeric(String value) {
    short s = Short.parseShort(value, 8);
    return new Mode(s);
  }

  private Mode parseSymbolic(String value) {
    Mode.Bits ownerBits = Bits.NONE;
    Mode.Bits groupBits = Bits.NONE;
    Mode.Bits otherBits = Bits.NONE;

    String[] specs = value.contains(",") ? value.split(",") : new String[] { value };
    for (String spec : specs) {
      String[] specParts = spec.split("=");
      Preconditions.checkArgument(specParts.length == 2,
          "Invalid mode %s - contains invalid segment %s", value, spec);

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
            // TODO(rvesse) Report error
        }
      }

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
          default:
            // TODO(rvesse) Report error
        }
      }
    }

    return new Mode(ownerBits, groupBits, otherBits);
  }
}
