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

public enum Status implements org.apache.thrift.TEnum {
  UNKNOWN(0),
  CREATED(1),
  CANCELED(2),
  FAILED(3),
  RUNNING(4),
  COMPLETED(5);

  private final int value;

  private Status(int value) {
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
  public static Status findByValue(int value) { 
    switch (value) {
      case 0:
        return UNKNOWN;
      case 1:
        return CREATED;
      case 2:
        return CANCELED;
      case 3:
        return FAILED;
      case 4:
        return RUNNING;
      case 5:
        return COMPLETED;
      default:
        return null;
    }
  }
}
