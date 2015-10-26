/**
 * Autogenerated by Thrift Compiler (0.9.2)
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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-10-26")
public class LineageCommand implements org.apache.thrift.TBase<LineageCommand, LineageCommand._Fields>, java.io.Serializable, Cloneable, Comparable<LineageCommand> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LineageCommand");

  private static final org.apache.thrift.protocol.TField COMMAND_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("commandType", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField CHECKPOINT_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField("checkpointFiles", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LineageCommandStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LineageCommandTupleSchemeFactory());
  }

  /**
   * 
   * @see tachyon.thrift.CommandType
   */
  public tachyon.thrift.CommandType commandType; // required
  public List<CheckpointFile> checkpointFiles; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see tachyon.thrift.CommandType
     */
    COMMAND_TYPE((short)1, "commandType"),
    CHECKPOINT_FILES((short)2, "checkpointFiles");

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
        case 1: // COMMAND_TYPE
          return COMMAND_TYPE;
        case 2: // CHECKPOINT_FILES
          return CHECKPOINT_FILES;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COMMAND_TYPE, new org.apache.thrift.meta_data.FieldMetaData("commandType", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, tachyon.thrift.CommandType.class)));
    tmpMap.put(_Fields.CHECKPOINT_FILES, new org.apache.thrift.meta_data.FieldMetaData("checkpointFiles", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, CheckpointFile.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LineageCommand.class, metaDataMap);
  }

  public LineageCommand() {
  }

  public LineageCommand(
    tachyon.thrift.CommandType commandType,
    List<CheckpointFile> checkpointFiles)
  {
    this();
    this.commandType = commandType;
    this.checkpointFiles = checkpointFiles;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LineageCommand(LineageCommand other) {
    if (other.isSetCommandType()) {
      this.commandType = other.commandType;
    }
    if (other.isSetCheckpointFiles()) {
      List<CheckpointFile> __this__checkpointFiles = new ArrayList<CheckpointFile>(other.checkpointFiles.size());
      for (CheckpointFile other_element : other.checkpointFiles) {
        __this__checkpointFiles.add(new CheckpointFile(other_element));
      }
      this.checkpointFiles = __this__checkpointFiles;
    }
  }

  public LineageCommand deepCopy() {
    return new LineageCommand(this);
  }

  @Override
  public void clear() {
    this.commandType = null;
    this.checkpointFiles = null;
  }

  /**
   * 
   * @see tachyon.thrift.CommandType
   */
  public tachyon.thrift.CommandType getCommandType() {
    return this.commandType;
  }

  /**
   * 
   * @see tachyon.thrift.CommandType
   */
  public LineageCommand setCommandType(tachyon.thrift.CommandType commandType) {
    this.commandType = commandType;
    return this;
  }

  public void unsetCommandType() {
    this.commandType = null;
  }

  /** Returns true if field commandType is set (has been assigned a value) and false otherwise */
  public boolean isSetCommandType() {
    return this.commandType != null;
  }

  public void setCommandTypeIsSet(boolean value) {
    if (!value) {
      this.commandType = null;
    }
  }

  public int getCheckpointFilesSize() {
    return (this.checkpointFiles == null) ? 0 : this.checkpointFiles.size();
  }

  public java.util.Iterator<CheckpointFile> getCheckpointFilesIterator() {
    return (this.checkpointFiles == null) ? null : this.checkpointFiles.iterator();
  }

  public void addToCheckpointFiles(CheckpointFile elem) {
    if (this.checkpointFiles == null) {
      this.checkpointFiles = new ArrayList<CheckpointFile>();
    }
    this.checkpointFiles.add(elem);
  }

  public List<CheckpointFile> getCheckpointFiles() {
    return this.checkpointFiles;
  }

  public LineageCommand setCheckpointFiles(List<CheckpointFile> checkpointFiles) {
    this.checkpointFiles = checkpointFiles;
    return this;
  }

  public void unsetCheckpointFiles() {
    this.checkpointFiles = null;
  }

  /** Returns true if field checkpointFiles is set (has been assigned a value) and false otherwise */
  public boolean isSetCheckpointFiles() {
    return this.checkpointFiles != null;
  }

  public void setCheckpointFilesIsSet(boolean value) {
    if (!value) {
      this.checkpointFiles = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COMMAND_TYPE:
      if (value == null) {
        unsetCommandType();
      } else {
        setCommandType((tachyon.thrift.CommandType)value);
      }
      break;

    case CHECKPOINT_FILES:
      if (value == null) {
        unsetCheckpointFiles();
      } else {
        setCheckpointFiles((List<CheckpointFile>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COMMAND_TYPE:
      return getCommandType();

    case CHECKPOINT_FILES:
      return getCheckpointFiles();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COMMAND_TYPE:
      return isSetCommandType();
    case CHECKPOINT_FILES:
      return isSetCheckpointFiles();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LineageCommand)
      return this.equals((LineageCommand)that);
    return false;
  }

  public boolean equals(LineageCommand that) {
    if (that == null)
      return false;

    boolean this_present_commandType = true && this.isSetCommandType();
    boolean that_present_commandType = true && that.isSetCommandType();
    if (this_present_commandType || that_present_commandType) {
      if (!(this_present_commandType && that_present_commandType))
        return false;
      if (!this.commandType.equals(that.commandType))
        return false;
    }

    boolean this_present_checkpointFiles = true && this.isSetCheckpointFiles();
    boolean that_present_checkpointFiles = true && that.isSetCheckpointFiles();
    if (this_present_checkpointFiles || that_present_checkpointFiles) {
      if (!(this_present_checkpointFiles && that_present_checkpointFiles))
        return false;
      if (!this.checkpointFiles.equals(that.checkpointFiles))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_commandType = true && (isSetCommandType());
    list.add(present_commandType);
    if (present_commandType)
      list.add(commandType.getValue());

    boolean present_checkpointFiles = true && (isSetCheckpointFiles());
    list.add(present_checkpointFiles);
    if (present_checkpointFiles)
      list.add(checkpointFiles);

    return list.hashCode();
  }

  @Override
  public int compareTo(LineageCommand other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCommandType()).compareTo(other.isSetCommandType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCommandType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.commandType, other.commandType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCheckpointFiles()).compareTo(other.isSetCheckpointFiles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCheckpointFiles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.checkpointFiles, other.checkpointFiles);
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
    StringBuilder sb = new StringBuilder("LineageCommand(");
    boolean first = true;

    sb.append("commandType:");
    if (this.commandType == null) {
      sb.append("null");
    } else {
      sb.append(this.commandType);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("checkpointFiles:");
    if (this.checkpointFiles == null) {
      sb.append("null");
    } else {
      sb.append(this.checkpointFiles);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LineageCommandStandardSchemeFactory implements SchemeFactory {
    public LineageCommandStandardScheme getScheme() {
      return new LineageCommandStandardScheme();
    }
  }

  private static class LineageCommandStandardScheme extends StandardScheme<LineageCommand> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LineageCommand struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COMMAND_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.commandType = tachyon.thrift.CommandType.findByValue(iprot.readI32());
              struct.setCommandTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CHECKPOINT_FILES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.checkpointFiles = new ArrayList<CheckpointFile>(_list8.size);
                CheckpointFile _elem9;
                for (int _i10 = 0; _i10 < _list8.size; ++_i10)
                {
                  _elem9 = new CheckpointFile();
                  _elem9.read(iprot);
                  struct.checkpointFiles.add(_elem9);
                }
                iprot.readListEnd();
              }
              struct.setCheckpointFilesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LineageCommand struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.commandType != null) {
        oprot.writeFieldBegin(COMMAND_TYPE_FIELD_DESC);
        oprot.writeI32(struct.commandType.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.checkpointFiles != null) {
        oprot.writeFieldBegin(CHECKPOINT_FILES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.checkpointFiles.size()));
          for (CheckpointFile _iter11 : struct.checkpointFiles)
          {
            _iter11.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LineageCommandTupleSchemeFactory implements SchemeFactory {
    public LineageCommandTupleScheme getScheme() {
      return new LineageCommandTupleScheme();
    }
  }

  private static class LineageCommandTupleScheme extends TupleScheme<LineageCommand> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LineageCommand struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetCommandType()) {
        optionals.set(0);
      }
      if (struct.isSetCheckpointFiles()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetCommandType()) {
        oprot.writeI32(struct.commandType.getValue());
      }
      if (struct.isSetCheckpointFiles()) {
        {
          oprot.writeI32(struct.checkpointFiles.size());
          for (CheckpointFile _iter12 : struct.checkpointFiles)
          {
            _iter12.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LineageCommand struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.commandType = tachyon.thrift.CommandType.findByValue(iprot.readI32());
        struct.setCommandTypeIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.checkpointFiles = new ArrayList<CheckpointFile>(_list13.size);
          CheckpointFile _elem14;
          for (int _i15 = 0; _i15 < _list13.size; ++_i15)
          {
            _elem14 = new CheckpointFile();
            _elem14.read(iprot);
            struct.checkpointFiles.add(_elem14);
          }
        }
        struct.setCheckpointFilesIsSet(true);
      }
    }
  }

}

