/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package alluxio.thrift;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-02-19")
public class CreateFileTOptions implements org.apache.thrift.TBase<CreateFileTOptions, CreateFileTOptions._Fields>, java.io.Serializable, Cloneable, Comparable<CreateFileTOptions> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CreateFileTOptions");

  private static final org.apache.thrift.protocol.TField BLOCK_SIZE_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("blockSizeBytes", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField PERSISTED_FIELD_DESC = new org.apache.thrift.protocol.TField("persisted", org.apache.thrift.protocol.TType.BOOL, (short)2);
  private static final org.apache.thrift.protocol.TField RECURSIVE_FIELD_DESC = new org.apache.thrift.protocol.TField("recursive", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField TTL_FIELD_DESC = new org.apache.thrift.protocol.TField("ttl", org.apache.thrift.protocol.TType.I64, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CreateFileTOptionsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CreateFileTOptionsTupleSchemeFactory());
  }

  private long blockSizeBytes; // optional
  private boolean persisted; // optional
  private boolean recursive; // optional
  private long ttl; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOCK_SIZE_BYTES((short)1, "blockSizeBytes"),
    PERSISTED((short)2, "persisted"),
    RECURSIVE((short)3, "recursive"),
    TTL((short)4, "ttl");

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
        case 1: // BLOCK_SIZE_BYTES
          return BLOCK_SIZE_BYTES;
        case 2: // PERSISTED
          return PERSISTED;
        case 3: // RECURSIVE
          return RECURSIVE;
        case 4: // TTL
          return TTL;
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
  private static final int __BLOCKSIZEBYTES_ISSET_ID = 0;
  private static final int __PERSISTED_ISSET_ID = 1;
  private static final int __RECURSIVE_ISSET_ID = 2;
  private static final int __TTL_ISSET_ID = 3;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.BLOCK_SIZE_BYTES,_Fields.PERSISTED,_Fields.RECURSIVE,_Fields.TTL};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOCK_SIZE_BYTES, new org.apache.thrift.meta_data.FieldMetaData("blockSizeBytes", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.PERSISTED, new org.apache.thrift.meta_data.FieldMetaData("persisted", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.RECURSIVE, new org.apache.thrift.meta_data.FieldMetaData("recursive", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.TTL, new org.apache.thrift.meta_data.FieldMetaData("ttl", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CreateFileTOptions.class, metaDataMap);
  }

  public CreateFileTOptions() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CreateFileTOptions(CreateFileTOptions other) {
    __isset_bitfield = other.__isset_bitfield;
    this.blockSizeBytes = other.blockSizeBytes;
    this.persisted = other.persisted;
    this.recursive = other.recursive;
    this.ttl = other.ttl;
  }

  public CreateFileTOptions deepCopy() {
    return new CreateFileTOptions(this);
  }

  @Override
  public void clear() {
    setBlockSizeBytesIsSet(false);
    this.blockSizeBytes = 0;
    setPersistedIsSet(false);
    this.persisted = false;
    setRecursiveIsSet(false);
    this.recursive = false;
    setTtlIsSet(false);
    this.ttl = 0;
  }

  public long getBlockSizeBytes() {
    return this.blockSizeBytes;
  }

  public CreateFileTOptions setBlockSizeBytes(long blockSizeBytes) {
    this.blockSizeBytes = blockSizeBytes;
    setBlockSizeBytesIsSet(true);
    return this;
  }

  public void unsetBlockSizeBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID);
  }

  /** Returns true if field blockSizeBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockSizeBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID);
  }

  public void setBlockSizeBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BLOCKSIZEBYTES_ISSET_ID, value);
  }

  public boolean isPersisted() {
    return this.persisted;
  }

  public CreateFileTOptions setPersisted(boolean persisted) {
    this.persisted = persisted;
    setPersistedIsSet(true);
    return this;
  }

  public void unsetPersisted() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PERSISTED_ISSET_ID);
  }

  /** Returns true if field persisted is set (has been assigned a value) and false otherwise */
  public boolean isSetPersisted() {
    return EncodingUtils.testBit(__isset_bitfield, __PERSISTED_ISSET_ID);
  }

  public void setPersistedIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PERSISTED_ISSET_ID, value);
  }

  public boolean isRecursive() {
    return this.recursive;
  }

  public CreateFileTOptions setRecursive(boolean recursive) {
    this.recursive = recursive;
    setRecursiveIsSet(true);
    return this;
  }

  public void unsetRecursive() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __RECURSIVE_ISSET_ID);
  }

  /** Returns true if field recursive is set (has been assigned a value) and false otherwise */
  public boolean isSetRecursive() {
    return EncodingUtils.testBit(__isset_bitfield, __RECURSIVE_ISSET_ID);
  }

  public void setRecursiveIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __RECURSIVE_ISSET_ID, value);
  }

  public long getTtl() {
    return this.ttl;
  }

  public CreateFileTOptions setTtl(long ttl) {
    this.ttl = ttl;
    setTtlIsSet(true);
    return this;
  }

  public void unsetTtl() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TTL_ISSET_ID);
  }

  /** Returns true if field ttl is set (has been assigned a value) and false otherwise */
  public boolean isSetTtl() {
    return EncodingUtils.testBit(__isset_bitfield, __TTL_ISSET_ID);
  }

  public void setTtlIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TTL_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOCK_SIZE_BYTES:
      if (value == null) {
        unsetBlockSizeBytes();
      } else {
        setBlockSizeBytes((Long)value);
      }
      break;

    case PERSISTED:
      if (value == null) {
        unsetPersisted();
      } else {
        setPersisted((Boolean)value);
      }
      break;

    case RECURSIVE:
      if (value == null) {
        unsetRecursive();
      } else {
        setRecursive((Boolean)value);
      }
      break;

    case TTL:
      if (value == null) {
        unsetTtl();
      } else {
        setTtl((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOCK_SIZE_BYTES:
      return getBlockSizeBytes();

    case PERSISTED:
      return isPersisted();

    case RECURSIVE:
      return isRecursive();

    case TTL:
      return getTtl();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BLOCK_SIZE_BYTES:
      return isSetBlockSizeBytes();
    case PERSISTED:
      return isSetPersisted();
    case RECURSIVE:
      return isSetRecursive();
    case TTL:
      return isSetTtl();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CreateFileTOptions)
      return this.equals((CreateFileTOptions)that);
    return false;
  }

  public boolean equals(CreateFileTOptions that) {
    if (that == null)
      return false;

    boolean this_present_blockSizeBytes = true && this.isSetBlockSizeBytes();
    boolean that_present_blockSizeBytes = true && that.isSetBlockSizeBytes();
    if (this_present_blockSizeBytes || that_present_blockSizeBytes) {
      if (!(this_present_blockSizeBytes && that_present_blockSizeBytes))
        return false;
      if (this.blockSizeBytes != that.blockSizeBytes)
        return false;
    }

    boolean this_present_persisted = true && this.isSetPersisted();
    boolean that_present_persisted = true && that.isSetPersisted();
    if (this_present_persisted || that_present_persisted) {
      if (!(this_present_persisted && that_present_persisted))
        return false;
      if (this.persisted != that.persisted)
        return false;
    }

    boolean this_present_recursive = true && this.isSetRecursive();
    boolean that_present_recursive = true && that.isSetRecursive();
    if (this_present_recursive || that_present_recursive) {
      if (!(this_present_recursive && that_present_recursive))
        return false;
      if (this.recursive != that.recursive)
        return false;
    }

    boolean this_present_ttl = true && this.isSetTtl();
    boolean that_present_ttl = true && that.isSetTtl();
    if (this_present_ttl || that_present_ttl) {
      if (!(this_present_ttl && that_present_ttl))
        return false;
      if (this.ttl != that.ttl)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_blockSizeBytes = true && (isSetBlockSizeBytes());
    list.add(present_blockSizeBytes);
    if (present_blockSizeBytes)
      list.add(blockSizeBytes);

    boolean present_persisted = true && (isSetPersisted());
    list.add(present_persisted);
    if (present_persisted)
      list.add(persisted);

    boolean present_recursive = true && (isSetRecursive());
    list.add(present_recursive);
    if (present_recursive)
      list.add(recursive);

    boolean present_ttl = true && (isSetTtl());
    list.add(present_ttl);
    if (present_ttl)
      list.add(ttl);

    return list.hashCode();
  }

  @Override
  public int compareTo(CreateFileTOptions other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBlockSizeBytes()).compareTo(other.isSetBlockSizeBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockSizeBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockSizeBytes, other.blockSizeBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPersisted()).compareTo(other.isSetPersisted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPersisted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.persisted, other.persisted);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRecursive()).compareTo(other.isSetRecursive());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRecursive()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.recursive, other.recursive);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTtl()).compareTo(other.isSetTtl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTtl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ttl, other.ttl);
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
    StringBuilder sb = new StringBuilder("CreateFileTOptions(");
    boolean first = true;

    if (isSetBlockSizeBytes()) {
      sb.append("blockSizeBytes:");
      sb.append(this.blockSizeBytes);
      first = false;
    }
    if (isSetPersisted()) {
      if (!first) sb.append(", ");
      sb.append("persisted:");
      sb.append(this.persisted);
      first = false;
    }
    if (isSetRecursive()) {
      if (!first) sb.append(", ");
      sb.append("recursive:");
      sb.append(this.recursive);
      first = false;
    }
    if (isSetTtl()) {
      if (!first) sb.append(", ");
      sb.append("ttl:");
      sb.append(this.ttl);
      first = false;
    }
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

  private static class CreateFileTOptionsStandardSchemeFactory implements SchemeFactory {
    public CreateFileTOptionsStandardScheme getScheme() {
      return new CreateFileTOptionsStandardScheme();
    }
  }

  private static class CreateFileTOptionsStandardScheme extends StandardScheme<CreateFileTOptions> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BLOCK_SIZE_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blockSizeBytes = iprot.readI64();
              struct.setBlockSizeBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PERSISTED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.persisted = iprot.readBool();
              struct.setPersistedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // RECURSIVE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.recursive = iprot.readBool();
              struct.setRecursiveIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TTL
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.ttl = iprot.readI64();
              struct.setTtlIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.isSetBlockSizeBytes()) {
        oprot.writeFieldBegin(BLOCK_SIZE_BYTES_FIELD_DESC);
        oprot.writeI64(struct.blockSizeBytes);
        oprot.writeFieldEnd();
      }
      if (struct.isSetPersisted()) {
        oprot.writeFieldBegin(PERSISTED_FIELD_DESC);
        oprot.writeBool(struct.persisted);
        oprot.writeFieldEnd();
      }
      if (struct.isSetRecursive()) {
        oprot.writeFieldBegin(RECURSIVE_FIELD_DESC);
        oprot.writeBool(struct.recursive);
        oprot.writeFieldEnd();
      }
      if (struct.isSetTtl()) {
        oprot.writeFieldBegin(TTL_FIELD_DESC);
        oprot.writeI64(struct.ttl);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CreateFileTOptionsTupleSchemeFactory implements SchemeFactory {
    public CreateFileTOptionsTupleScheme getScheme() {
      return new CreateFileTOptionsTupleScheme();
    }
  }

  private static class CreateFileTOptionsTupleScheme extends TupleScheme<CreateFileTOptions> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBlockSizeBytes()) {
        optionals.set(0);
      }
      if (struct.isSetPersisted()) {
        optionals.set(1);
      }
      if (struct.isSetRecursive()) {
        optionals.set(2);
      }
      if (struct.isSetTtl()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetBlockSizeBytes()) {
        oprot.writeI64(struct.blockSizeBytes);
      }
      if (struct.isSetPersisted()) {
        oprot.writeBool(struct.persisted);
      }
      if (struct.isSetRecursive()) {
        oprot.writeBool(struct.recursive);
      }
      if (struct.isSetTtl()) {
        oprot.writeI64(struct.ttl);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CreateFileTOptions struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.blockSizeBytes = iprot.readI64();
        struct.setBlockSizeBytesIsSet(true);
      }
      if (incoming.get(1)) {
        struct.persisted = iprot.readBool();
        struct.setPersistedIsSet(true);
      }
      if (incoming.get(2)) {
        struct.recursive = iprot.readBool();
        struct.setRecursiveIsSet(true);
      }
      if (incoming.get(3)) {
        struct.ttl = iprot.readI64();
        struct.setTtlIsSet(true);
      }
    }
  }

}

