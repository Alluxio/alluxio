/**
 * Autogenerated by Thrift Compiler (0.9.1)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientBlockInfo implements org.apache.thrift.TBase<ClientBlockInfo, ClientBlockInfo._Fields>, java.io.Serializable, Cloneable, Comparable<ClientBlockInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ClientBlockInfo");

  private static final org.apache.thrift.protocol.TField BLOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blockId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField OFFSET_FIELD_DESC = new org.apache.thrift.protocol.TField("offset", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField LENGTH_FIELD_DESC = new org.apache.thrift.protocol.TField("length", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField WORKERS_FIELD_DESC = new org.apache.thrift.protocol.TField("workers", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField CHECKPOINTS_FIELD_DESC = new org.apache.thrift.protocol.TField("checkpoints", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ClientBlockInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ClientBlockInfoTupleSchemeFactory());
  }

  public long blockId; // required
  public long offset; // required
  public long length; // required
  public List<WorkerInfo> workers; // required
  public List<String> checkpoints; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BLOCK_ID((short)1, "blockId"),
    OFFSET((short)2, "offset"),
    LENGTH((short)3, "length"),
    WORKERS((short)4, "workers"),
    CHECKPOINTS((short)5, "checkpoints");

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
        case 1: // BLOCK_ID
          return BLOCK_ID;
        case 2: // OFFSET
          return OFFSET;
        case 3: // LENGTH
          return LENGTH;
        case 4: // WORKERS
          return WORKERS;
        case 5: // CHECKPOINTS
          return CHECKPOINTS;
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
  private static final int __BLOCKID_ISSET_ID = 0;
  private static final int __OFFSET_ISSET_ID = 1;
  private static final int __LENGTH_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BLOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("blockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.OFFSET, new org.apache.thrift.meta_data.FieldMetaData("offset", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.LENGTH, new org.apache.thrift.meta_data.FieldMetaData("length", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.WORKERS, new org.apache.thrift.meta_data.FieldMetaData("workers", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, WorkerInfo.class))));
    tmpMap.put(_Fields.CHECKPOINTS, new org.apache.thrift.meta_data.FieldMetaData("checkpoints", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ClientBlockInfo.class, metaDataMap);
  }

  public ClientBlockInfo() {
  }

  public ClientBlockInfo(
    long blockId,
    long offset,
    long length,
    List<WorkerInfo> workers,
    List<String> checkpoints)
  {
    this();
    this.blockId = blockId;
    setBlockIdIsSet(true);
    this.offset = offset;
    setOffsetIsSet(true);
    this.length = length;
    setLengthIsSet(true);
    this.workers = workers;
    this.checkpoints = checkpoints;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ClientBlockInfo(ClientBlockInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.blockId = other.blockId;
    this.offset = other.offset;
    this.length = other.length;
    if (other.isSetWorkers()) {
      List<WorkerInfo> __this__workers = new ArrayList<WorkerInfo>(other.workers.size());
      for (WorkerInfo other_element : other.workers) {
        __this__workers.add(new WorkerInfo(other_element));
      }
      this.workers = __this__workers;
    }
    if (other.isSetCheckpoints()) {
      List<String> __this__checkpoints = new ArrayList<String>(other.checkpoints);
      this.checkpoints = __this__checkpoints;
    }
  }

  public ClientBlockInfo deepCopy() {
    return new ClientBlockInfo(this);
  }

  @Override
  public void clear() {
    setBlockIdIsSet(false);
    this.blockId = 0;
    setOffsetIsSet(false);
    this.offset = 0;
    setLengthIsSet(false);
    this.length = 0;
    this.workers = null;
    this.checkpoints = null;
  }

  public long getBlockId() {
    return this.blockId;
  }

  public ClientBlockInfo setBlockId(long blockId) {
    this.blockId = blockId;
    setBlockIdIsSet(true);
    return this;
  }

  public void unsetBlockId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BLOCKID_ISSET_ID);
  }

  /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockId() {
    return EncodingUtils.testBit(__isset_bitfield, __BLOCKID_ISSET_ID);
  }

  public void setBlockIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BLOCKID_ISSET_ID, value);
  }

  public long getOffset() {
    return this.offset;
  }

  public ClientBlockInfo setOffset(long offset) {
    this.offset = offset;
    setOffsetIsSet(true);
    return this;
  }

  public void unsetOffset() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __OFFSET_ISSET_ID);
  }

  /** Returns true if field offset is set (has been assigned a value) and false otherwise */
  public boolean isSetOffset() {
    return EncodingUtils.testBit(__isset_bitfield, __OFFSET_ISSET_ID);
  }

  public void setOffsetIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __OFFSET_ISSET_ID, value);
  }

  public long getLength() {
    return this.length;
  }

  public ClientBlockInfo setLength(long length) {
    this.length = length;
    setLengthIsSet(true);
    return this;
  }

  public void unsetLength() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __LENGTH_ISSET_ID);
  }

  /** Returns true if field length is set (has been assigned a value) and false otherwise */
  public boolean isSetLength() {
    return EncodingUtils.testBit(__isset_bitfield, __LENGTH_ISSET_ID);
  }

  public void setLengthIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __LENGTH_ISSET_ID, value);
  }

  public int getWorkersSize() {
    return (this.workers == null) ? 0 : this.workers.size();
  }

  public java.util.Iterator<WorkerInfo> getWorkersIterator() {
    return (this.workers == null) ? null : this.workers.iterator();
  }

  public void addToWorkers(WorkerInfo elem) {
    if (this.workers == null) {
      this.workers = new ArrayList<WorkerInfo>();
    }
    this.workers.add(elem);
  }

  public List<WorkerInfo> getWorkers() {
    return this.workers;
  }

  public ClientBlockInfo setWorkers(List<WorkerInfo> workers) {
    this.workers = workers;
    return this;
  }

  public void unsetWorkers() {
    this.workers = null;
  }

  /** Returns true if field workers is set (has been assigned a value) and false otherwise */
  public boolean isSetWorkers() {
    return this.workers != null;
  }

  public void setWorkersIsSet(boolean value) {
    if (!value) {
      this.workers = null;
    }
  }

  public int getCheckpointsSize() {
    return (this.checkpoints == null) ? 0 : this.checkpoints.size();
  }

  public java.util.Iterator<String> getCheckpointsIterator() {
    return (this.checkpoints == null) ? null : this.checkpoints.iterator();
  }

  public void addToCheckpoints(String elem) {
    if (this.checkpoints == null) {
      this.checkpoints = new ArrayList<String>();
    }
    this.checkpoints.add(elem);
  }

  public List<String> getCheckpoints() {
    return this.checkpoints;
  }

  public ClientBlockInfo setCheckpoints(List<String> checkpoints) {
    this.checkpoints = checkpoints;
    return this;
  }

  public void unsetCheckpoints() {
    this.checkpoints = null;
  }

  /** Returns true if field checkpoints is set (has been assigned a value) and false otherwise */
  public boolean isSetCheckpoints() {
    return this.checkpoints != null;
  }

  public void setCheckpointsIsSet(boolean value) {
    if (!value) {
      this.checkpoints = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case BLOCK_ID:
      if (value == null) {
        unsetBlockId();
      } else {
        setBlockId((Long)value);
      }
      break;

    case OFFSET:
      if (value == null) {
        unsetOffset();
      } else {
        setOffset((Long)value);
      }
      break;

    case LENGTH:
      if (value == null) {
        unsetLength();
      } else {
        setLength((Long)value);
      }
      break;

    case WORKERS:
      if (value == null) {
        unsetWorkers();
      } else {
        setWorkers((List<WorkerInfo>)value);
      }
      break;

    case CHECKPOINTS:
      if (value == null) {
        unsetCheckpoints();
      } else {
        setCheckpoints((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case BLOCK_ID:
      return Long.valueOf(getBlockId());

    case OFFSET:
      return Long.valueOf(getOffset());

    case LENGTH:
      return Long.valueOf(getLength());

    case WORKERS:
      return getWorkers();

    case CHECKPOINTS:
      return getCheckpoints();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case BLOCK_ID:
      return isSetBlockId();
    case OFFSET:
      return isSetOffset();
    case LENGTH:
      return isSetLength();
    case WORKERS:
      return isSetWorkers();
    case CHECKPOINTS:
      return isSetCheckpoints();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ClientBlockInfo)
      return this.equals((ClientBlockInfo)that);
    return false;
  }

  public boolean equals(ClientBlockInfo that) {
    if (that == null)
      return false;

    boolean this_present_blockId = true;
    boolean that_present_blockId = true;
    if (this_present_blockId || that_present_blockId) {
      if (!(this_present_blockId && that_present_blockId))
        return false;
      if (this.blockId != that.blockId)
        return false;
    }

    boolean this_present_offset = true;
    boolean that_present_offset = true;
    if (this_present_offset || that_present_offset) {
      if (!(this_present_offset && that_present_offset))
        return false;
      if (this.offset != that.offset)
        return false;
    }

    boolean this_present_length = true;
    boolean that_present_length = true;
    if (this_present_length || that_present_length) {
      if (!(this_present_length && that_present_length))
        return false;
      if (this.length != that.length)
        return false;
    }

    boolean this_present_workers = true && this.isSetWorkers();
    boolean that_present_workers = true && that.isSetWorkers();
    if (this_present_workers || that_present_workers) {
      if (!(this_present_workers && that_present_workers))
        return false;
      if (!this.workers.equals(that.workers))
        return false;
    }

    boolean this_present_checkpoints = true && this.isSetCheckpoints();
    boolean that_present_checkpoints = true && that.isSetCheckpoints();
    if (this_present_checkpoints || that_present_checkpoints) {
      if (!(this_present_checkpoints && that_present_checkpoints))
        return false;
      if (!this.checkpoints.equals(that.checkpoints))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ClientBlockInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetBlockId()).compareTo(other.isSetBlockId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockId, other.blockId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOffset()).compareTo(other.isSetOffset());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOffset()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.offset, other.offset);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLength()).compareTo(other.isSetLength());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLength()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.length, other.length);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWorkers()).compareTo(other.isSetWorkers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWorkers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.workers, other.workers);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCheckpoints()).compareTo(other.isSetCheckpoints());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCheckpoints()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.checkpoints, other.checkpoints);
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
    StringBuilder sb = new StringBuilder("ClientBlockInfo(");
    boolean first = true;

    sb.append("blockId:");
    sb.append(this.blockId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("offset:");
    sb.append(this.offset);
    first = false;
    if (!first) sb.append(", ");
    sb.append("length:");
    sb.append(this.length);
    first = false;
    if (!first) sb.append(", ");
    sb.append("workers:");
    if (this.workers == null) {
      sb.append("null");
    } else {
      sb.append(this.workers);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("checkpoints:");
    if (this.checkpoints == null) {
      sb.append("null");
    } else {
      sb.append(this.checkpoints);
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

  private static class ClientBlockInfoStandardSchemeFactory implements SchemeFactory {
    public ClientBlockInfoStandardScheme getScheme() {
      return new ClientBlockInfoStandardScheme();
    }
  }

  private static class ClientBlockInfoStandardScheme extends StandardScheme<ClientBlockInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ClientBlockInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BLOCK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blockId = iprot.readI64();
              struct.setBlockIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // OFFSET
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.offset = iprot.readI64();
              struct.setOffsetIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LENGTH
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.length = iprot.readI64();
              struct.setLengthIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // WORKERS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.workers = new ArrayList<WorkerInfo>(_list8.size);
                for (int _i9 = 0; _i9 < _list8.size; ++_i9)
                {
                  WorkerInfo _elem10;
                  _elem10 = new WorkerInfo();
                  _elem10.read(iprot);
                  struct.workers.add(_elem10);
                }
                iprot.readListEnd();
              }
              struct.setWorkersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // CHECKPOINTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list11 = iprot.readListBegin();
                struct.checkpoints = new ArrayList<String>(_list11.size);
                for (int _i12 = 0; _i12 < _list11.size; ++_i12)
                {
                  String _elem13;
                  _elem13 = iprot.readString();
                  struct.checkpoints.add(_elem13);
                }
                iprot.readListEnd();
              }
              struct.setCheckpointsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ClientBlockInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(BLOCK_ID_FIELD_DESC);
      oprot.writeI64(struct.blockId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(OFFSET_FIELD_DESC);
      oprot.writeI64(struct.offset);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(LENGTH_FIELD_DESC);
      oprot.writeI64(struct.length);
      oprot.writeFieldEnd();
      if (struct.workers != null) {
        oprot.writeFieldBegin(WORKERS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.workers.size()));
          for (WorkerInfo _iter14 : struct.workers)
          {
            _iter14.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.checkpoints != null) {
        oprot.writeFieldBegin(CHECKPOINTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.checkpoints.size()));
          for (String _iter15 : struct.checkpoints)
          {
            oprot.writeString(_iter15);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ClientBlockInfoTupleSchemeFactory implements SchemeFactory {
    public ClientBlockInfoTupleScheme getScheme() {
      return new ClientBlockInfoTupleScheme();
    }
  }

  private static class ClientBlockInfoTupleScheme extends TupleScheme<ClientBlockInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ClientBlockInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetBlockId()) {
        optionals.set(0);
      }
      if (struct.isSetOffset()) {
        optionals.set(1);
      }
      if (struct.isSetLength()) {
        optionals.set(2);
      }
      if (struct.isSetWorkers()) {
        optionals.set(3);
      }
      if (struct.isSetCheckpoints()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetBlockId()) {
        oprot.writeI64(struct.blockId);
      }
      if (struct.isSetOffset()) {
        oprot.writeI64(struct.offset);
      }
      if (struct.isSetLength()) {
        oprot.writeI64(struct.length);
      }
      if (struct.isSetWorkers()) {
        {
          oprot.writeI32(struct.workers.size());
          for (WorkerInfo _iter16 : struct.workers)
          {
            _iter16.write(oprot);
          }
        }
      }
      if (struct.isSetCheckpoints()) {
        {
          oprot.writeI32(struct.checkpoints.size());
          for (String _iter17 : struct.checkpoints)
          {
            oprot.writeString(_iter17);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ClientBlockInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.blockId = iprot.readI64();
        struct.setBlockIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.offset = iprot.readI64();
        struct.setOffsetIsSet(true);
      }
      if (incoming.get(2)) {
        struct.length = iprot.readI64();
        struct.setLengthIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list18 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.workers = new ArrayList<WorkerInfo>(_list18.size);
          for (int _i19 = 0; _i19 < _list18.size; ++_i19)
          {
            WorkerInfo _elem20;
            _elem20 = new WorkerInfo();
            _elem20.read(iprot);
            struct.workers.add(_elem20);
          }
        }
        struct.setWorkersIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.checkpoints = new ArrayList<String>(_list21.size);
          for (int _i22 = 0; _i22 < _list21.size; ++_i22)
          {
            String _elem23;
            _elem23 = iprot.readString();
            struct.checkpoints.add(_elem23);
          }
        }
        struct.setCheckpointsIsSet(true);
      }
    }
  }

}

