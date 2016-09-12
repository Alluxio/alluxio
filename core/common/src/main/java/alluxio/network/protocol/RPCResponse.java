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

package alluxio.network.protocol;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for constructing RPC responses.
 */
@ThreadSafe
public abstract class RPCResponse extends RPCMessage {

  /**
   * The possible types of status for RPC responses. When modifying values,
   * {@link #statusToMessage(RPCResponse.Status)} must be updated for the appropriate status
   * messages.
   */
  public enum Status {
    // Success.
    SUCCESS(0),
    // Generic errors.
    UNEXPECTED_STATUS_CODE(1),
    DECODE_ERROR(2),
    UNKNOWN_MESSAGE_ERROR(3),
    // Specific errors.
    FILE_DNE(100),
    BLOCK_LOCK_ERROR(101),
    WRITE_ERROR(102),
    UFS_READ_FAILED(103),
    UFS_WRITE_FAILED(104),
    ;

    private static final String DEFAULT_ERROR_STRING = "Unknown error.";
    /** Mapping from short id to {@link Status}. */
    private static final Map<Short, Status> SHORT_TO_STATUS_MAP = new HashMap<>();
    /** Mapping from {@link Status} to status message. */
    private static final Map<Status, String> STATUS_TO_MESSAGE_MAP = new HashMap<>();
    static {
      // Populate the mappings.
      for (Status status : Status.values()) {
        SHORT_TO_STATUS_MAP.put(status.getId(), status);
        STATUS_TO_MESSAGE_MAP.put(status, statusToMessage(status));
      }
    }

    private final short mId;

    Status(int id) {
      mId = (short) id;
    }

    /**
     * Returns the short identifier of the status.
     *
     * @return the short representing the status
     */
    public short getId() {
      return mId;
    }

    /**
     * Returns the message for the status.
     *
     * @return A string representing the status
     */
    public String getMessage() {
      String message = STATUS_TO_MESSAGE_MAP.get(this);
      if (message == null) {
        return DEFAULT_ERROR_STRING;
      }
      return message;
    }

    /**
     * Returns the {@link Status} represented by the short.
     *
     * @param id the short representing a {@link Status}
     * @return the {@link Status} representing the given short
     */
    public static Status fromShort(short id) {
      Status status = SHORT_TO_STATUS_MAP.get(id);
      if (status == null) {
        return UNEXPECTED_STATUS_CODE;
      }
      return status;
    }

    /**
     * Returns the {@link String} message for a given {@link Status}.
     *
     * This method must be updated when a new Status is added.
     *
     * @param status the {@link Status} to get the message for
     * @return the String message for the status
     */
    private static String statusToMessage(Status status) {
      switch (status) {
        case SUCCESS:
          return "Success!";
        case UNEXPECTED_STATUS_CODE:
          return "There was an unexpected status code.";
        case DECODE_ERROR:
          return "Decode error. Possible Client/DataServer version incompatibility.";
        case UNKNOWN_MESSAGE_ERROR:
          return "Unknown message error. Possible Client/DataServer version incompatibility.";
        case FILE_DNE:
          return "File does not exist.";
        case BLOCK_LOCK_ERROR:
          return "Failed to lock block.";
        case WRITE_ERROR:
          return "Failed to write block.";
        default:
          return DEFAULT_ERROR_STRING;
      }
    }
  }

  /**
   * @return the status
   */
  public abstract Status getStatus();
}
