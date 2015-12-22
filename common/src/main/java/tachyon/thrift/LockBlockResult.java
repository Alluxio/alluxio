/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package tachyon.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2015-12-15")
public class LockBlockResult implements org.apache.thrift.TBase<LockBlockResult, LockBlockResult._Fields>, java.io.Serializable, Cloneable, Comparable<LockBlockResult> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LockBlockResult");

  private static final org.apache.thrift.protocol.TField LOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("lockId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField BLOCK_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("blockPath", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LockBlockResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LockBlockResultTupleSchemeFactory());
  }

  public long lockId; // required
  public String blockPath; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LOCK_ID((short)1, "lockId"),
    BLOCK_PATH((short)2, "blockPath");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // LOCK_ID
          return LOCK_ID;
        case 2: // BLOCK_PATH
          return BLOCK_PATH;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __LOCKID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("lockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.BLOCK_PATH, new org.apache.thrift.meta_data.FieldMetaData("blockPath", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LockBlockResult.class, metaDataMap);
  }

  public LockBlockResult() {
  }

  public LockBlockResult(
    long lockId,
    String blockPath)
  {
    this();
    this.lockId = lockId;
    setLockIdIsSet(true);
    this.blockPath = blockPath;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LockBlockResult(LockBlockResult other) {
    __isset_bitfield = other.__isset_bitfield;
    this.lockId = other.lockId;
    if (other.isSetBlockPath()) {
      this.blockPath = other.blockPath;
    }
  }

  public LockBlockResult deepCopy() {
    return new LockBlockResult(this);
  }

  @Override
  public void clear() {
    setLockIdIsSet(false);
    this.lockId = 0;
    this.blockPath = null;
  }

  public long getLockId() {
    return this.lockId;
  }

  public LockBlockResult setLockId(long lockId) {
    this.lockId = lockId;
    setLockIdIsSet(true);
    return this;
  }

  public void unsetLockId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LOCKID_ISSET_ID);
  }

  /** Returns true if field lockId is set (has been assigned a value) and false otherwise */
  public boolean isSetLockId() {
    return EncodingUtils.testBit(__isset_bitfield, __LOCKID_ISSET_ID);
  }

  public void setLockIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LOCKID_ISSET_ID, value);
  }

  public String getBlockPath() {
    return this.blockPath;
  }

  public LockBlockResult setBlockPath(String blockPath) {
    this.blockPath = blockPath;
    return this;
  }

  public void unsetBlockPath() {
    this.blockPath = null;
  }

  /** Returns true if field blockPath is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockPath() {
    return this.blockPath != null;
  }

  public void setBlockPathIsSet(boolean value) {
    if (!value) {
      this.blockPath = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case LOCK_ID:
      if (value == null) {
        unsetLockId();
      } else {
        setLockId((Long)value);
      }
      break;

    case BLOCK_PATH:
      if (value == null) {
        unsetBlockPath();
      } else {
        setBlockPath((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case LOCK_ID:
      return getLockId();

    case BLOCK_PATH:
      return getBlockPath();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case LOCK_ID:
      return isSetLockId();
    case BLOCK_PATH:
      return isSetBlockPath();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LockBlockResult)
      return this.equals((LockBlockResult)that);
    return false;
  }

  public boolean equals(LockBlockResult that) {
    if (that == null)
      return false;

    boolean this_present_lockId = true;
    boolean that_present_lockId = true;
    if (this_present_lockId || that_present_lockId) {
      if (!(this_present_lockId && that_present_lockId))
        return false;
      if (this.lockId != that.lockId)
        return false;
    }

    boolean this_present_blockPath = true && this.isSetBlockPath();
    boolean that_present_blockPath = true && that.isSetBlockPath();
    if (this_present_blockPath || that_present_blockPath) {
      if (!(this_present_blockPath && that_present_blockPath))
        return false;
      if (!this.blockPath.equals(that.blockPath))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_lockId = true;
    list.add(present_lockId);
    if (present_lockId)
      list.add(lockId);

    boolean present_blockPath = true && (isSetBlockPath());
    list.add(present_blockPath);
    if (present_blockPath)
      list.add(blockPath);

    return list.hashCode();
  }

  @Override
  public int compareTo(LockBlockResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetLockId()).compareTo(other.isSetLockId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLockId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lockId, other.lockId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBlockPath()).compareTo(other.isSetBlockPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockPath, other.blockPath);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LockBlockResult(");
    boolean first = true;

    sb.append("lockId:");
    sb.append(this.lockId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blockPath:");
    if (this.blockPath == null) {
      sb.append("null");
    } else {
      sb.append(this.blockPath);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LockBlockResultStandardSchemeFactory implements SchemeFactory {
    public LockBlockResultStandardScheme getScheme() {
      return new LockBlockResultStandardScheme();
    }
  }

  private static class LockBlockResultStandardScheme extends StandardScheme<LockBlockResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LockBlockResult struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // LOCK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.lockId = iprot.readI64();
              struct.setLockIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BLOCK_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.blockPath = iprot.readString();
              struct.setBlockPathIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, LockBlockResult struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(LOCK_ID_FIELD_DESC);
      oprot.writeI64(struct.lockId);
      oprot.writeFieldEnd();
      if (struct.blockPath != null) {
        oprot.writeFieldBegin(BLOCK_PATH_FIELD_DESC);
        oprot.writeString(struct.blockPath);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LockBlockResultTupleSchemeFactory implements SchemeFactory {
    public LockBlockResultTupleScheme getScheme() {
      return new LockBlockResultTupleScheme();
    }
  }

  private static class LockBlockResultTupleScheme extends TupleScheme<LockBlockResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LockBlockResult struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetLockId()) {
        optionals.set(0);
      }
      if (struct.isSetBlockPath()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetLockId()) {
        oprot.writeI64(struct.lockId);
      }
      if (struct.isSetBlockPath()) {
        oprot.writeString(struct.blockPath);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LockBlockResult struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.lockId = iprot.readI64();
        struct.setLockIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.blockPath = iprot.readString();
        struct.setBlockPathIsSet(true);
      }
    }
  }

}

