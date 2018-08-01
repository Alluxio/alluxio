/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum WorkerInfoField implements org.apache.thrift.TEnum {
  ADDRESS(0),
  CAPACITY_BYTES(1),
  CAPACITY_BYTES_ON_TIERS(2),
  ID(3),
  LAST_CONTACT_SEC(4),
  START_TIME_MS(5),
  STATE(6),
  USED_BYTES(7),
  USED_BYTES_ON_TIERS(8);

  private final int value;

  private WorkerInfoField(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static WorkerInfoField findByValue(int value) { 
    switch (value) {
      case 0:
        return ADDRESS;
      case 1:
        return CAPACITY_BYTES;
      case 2:
        return CAPACITY_BYTES_ON_TIERS;
      case 3:
        return ID;
      case 4:
        return LAST_CONTACT_SEC;
      case 5:
        return START_TIME_MS;
      case 6:
        return STATE;
      case 7:
        return USED_BYTES;
      case 8:
        return USED_BYTES_ON_TIERS;
      default:
        return null;
    }
  }
}
