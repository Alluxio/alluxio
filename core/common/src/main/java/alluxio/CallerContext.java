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

package alluxio;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * This class defining the caller context for auditing operations.
 */
public class CallerContext {
  public static final Charset SIGNATURE_ENCODING = StandardCharsets.UTF_8;

  // The caller context
  private final String mContext;
  // The caller's signature for validation
  private final byte[] mSignature;

  /**
   * Constructor.
   * @param builder A builder for CallerContext
   */
  public CallerContext(Builder builder) {
    mContext = builder.getContext();
    mSignature = builder.getSignature();
  }

  /**
   * @return the context
   */
  public String getContext() {
    return mContext;
  }

  /**
   * @return the signature
   */
  public byte[] getSignature() {
    return mSignature == null
        ? null : Arrays.copyOf(mSignature, mSignature.length);
  }

  /**
   * @return ture if the context is valid
   */
  public boolean isValid() {
    return mContext != null;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(mContext).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (obj.getClass() != getClass()) {
      return false;
    } else {
      CallerContext rhs = (CallerContext) obj;
      return new EqualsBuilder()
          .append(mContext, rhs.mContext)
          .append(mSignature, rhs.mSignature)
          .isEquals();
    }
  }

  @Override
  public String toString() {
    if (!isValid()) {
      return "";
    }
    String str = mContext;
    if (mSignature != null) {
      str += ":";
      str += new String(mSignature, SIGNATURE_ENCODING);
    }
    return str;
  }

  /**
   * CallerContext Builder.
   */
  public static final class Builder {
    private static final String KEY_VALUE_SEPARATOR = ":";
    private final String mFieldSeparator;
    private final StringBuilder mContextBuilder = new StringBuilder();
    private byte[] mSignature;

    private static final String ALLUXIO_CALLER_CONTEXT_SEPARATOR = ",";

    /**
     * Constructor.
     * @param context the caller context
     */
    public Builder(String context) {
      this(context, ALLUXIO_CALLER_CONTEXT_SEPARATOR);
    }

    /**
     * Constructor.
     * @param context the caller context
     * @param separator the separator
     */
    private Builder(String context, String separator) {
      if (isValid(context)) {
        mContextBuilder.append(context);
      }
      mFieldSeparator = separator;
    }

    /**
     * Whether the field is valid.
     * @param field one of the fields in context
     * @return true if the field is not null or empty
     */
    private boolean isValid(String field) {
      return field != null && field.length() > 0;
    }

    /**
     * @param signature the signature for caller context
     * @return the updated builder instance
     */
    public Builder setSignature(byte[] signature) {
      if (signature != null && signature.length > 0) {
        mSignature = Arrays.copyOf(signature, signature.length);
      }
      return this;
    }

    /**
     * Get the context.
     * For example, the context is "key1:value1,key2:value2".
     * @return the valid context or null
     */
    public String getContext() {
      return mContextBuilder.length() > 0 ? mContextBuilder.toString() : null;
    }

    /**
     * Get the signature.
     * @return the signature
     */
    public byte[] getSignature() {
      return mSignature;
    }

    /**
     * Append new field to the context.
     * @param field one of fields to append
     * @return the updated builder instance
     */
    public Builder append(String field) {
      if (isValid(field)) {
        if (mContextBuilder.length() > 0) {
          mContextBuilder.append(mFieldSeparator);
        }
        mContextBuilder.append(field);
      }
      return this;
    }

    /**
     * Append new field which contains key and value to the context.
     * @param key the key of field
     * @param value the value of field
     * @return the updated builder instance
     */
    public Builder append(String key, String value) {
      if (isValid(key) && isValid(value)) {
        if (mContextBuilder.length() > 0) {
          mContextBuilder.append(mFieldSeparator);
        }
        mContextBuilder.append(key).append(KEY_VALUE_SEPARATOR).append(value);
      }
      return this;
    }

    /**
     * Constructs an CallerContext with the values declared by this CallerContext.Builder.
     * @return the CallerContext
     */
    public CallerContext build() {
      return new CallerContext(this);
    }
  }

  private static final class CurrentCallerContextHolder {
    static final ThreadLocal<CallerContext> CALLER_CONTEXT =
        new InheritableThreadLocal<>();
  }

  /**
   *
   * @return the current CallerContext
   */
  public static CallerContext getCurrent() {
    return CurrentCallerContextHolder.CALLER_CONTEXT.get();
  }

  /**
   * @param callerContext current CullerContext
   */
  public static void setCurrent(CallerContext callerContext) {
    CurrentCallerContextHolder.CALLER_CONTEXT.set(callerContext);
  }
}
