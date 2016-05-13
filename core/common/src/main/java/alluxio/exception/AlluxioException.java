/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.exception;

import alluxio.thrift.AlluxioTException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * General {@link AlluxioException} used throughout the system. It must be able serialize itself to
 * the RPC framework and convert back without losing any necessary information.
 */
@ThreadSafe
public abstract class AlluxioException extends Exception {
  private static final long serialVersionUID = 2243833925609642384L;

  private final AlluxioExceptionType mType;

  /**
   * Constructs a {@link AlluxioException} with an exception type from a {@link AlluxioTException}.
   *
   * @param te the type of the exception
   */
  protected AlluxioException(AlluxioTException te) {
    super(te.getMessage());
    mType = AlluxioExceptionType.valueOf(te.getType());
  }

  /**
   * Constructs an {@link AlluxioException} with the given cause.
   *
   * @param type the exception type
   * @param cause the cause
   */
  protected AlluxioException(AlluxioExceptionType type, Throwable cause) {
    super(cause);
    mType = type;
  }

  /**
   * Constructs an {@link AlluxioException} with the given message.
   *
   * @param type the exception type
   * @param message the message
   */
  protected AlluxioException(AlluxioExceptionType type, String message) {
    super(message);
    mType = type;
  }

  /**
   * Constructs an {@link AlluxioException} with the given message and cause.
   *
   * @param type the exception type
   * @param message the message
   * @param cause the cause
   */
  protected AlluxioException(AlluxioExceptionType type, String message, Throwable cause) {
    super(message, cause);
    mType = type;
  }

  /**
   * Constructs a {@link AlluxioTException} from a {@link AlluxioException}.
   *
   * @return a {@link AlluxioTException} of the type of this exception
   */
  public AlluxioTException toAlluxioTException() {
    return new AlluxioTException(mType.name(), getMessage(), getClass().getName());
  }

  /**
   * Constructs a {@link AlluxioException} from a {@link AlluxioTException}.
   *
   * @param e the {@link AlluxioTException} to convert to a {@link AlluxioException}
   * @return a {@link AlluxioException} of the type specified in e, with the message specified in e
   */

  public static AlluxioException from(AlluxioTException e) {
    try {
      // For 1.0, the type returns the enum type, while for ALLUXIO 1.1 and after, the type contains
      // the exception class name
      Class<? extends AlluxioException> throwClass;
      if (e.isSetName()) {
        // version 1.1 or newer
        throwClass = (Class<? extends AlluxioException>) Class.forName(e.getName());
      } else {
        // version 1.0
        AlluxioExceptionType exceptionType = AlluxioExceptionType.valueOf(e.getType());
        throwClass = exceptionType.getExceptionClass();
      }
      return throwClass.getConstructor(String.class).newInstance(e.getMessage());
    } catch (ReflectiveOperationException reflectException) {
      String errorMessage = "Could not instantiate " + e.getType() + " with a String-only "
          + "constructor: " + reflectException.getMessage();
      throw new IllegalStateException(errorMessage, reflectException);
    }
  }
}
