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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-08-03")
public class CommandLineJobInfo implements org.apache.thrift.TBase<CommandLineJobInfo, CommandLineJobInfo._Fields>, java.io.Serializable, Cloneable, Comparable<CommandLineJobInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CommandLineJobInfo");

  private static final org.apache.thrift.protocol.TField COMMAND_FIELD_DESC = new org.apache.thrift.protocol.TField("command", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField CONF_FIELD_DESC = new org.apache.thrift.protocol.TField("conf", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CommandLineJobInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CommandLineJobInfoTupleSchemeFactory());
  }

  private String command; // required
  private JobConfInfo conf; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COMMAND((short)1, "command"),
    CONF((short)2, "conf");

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
        case 1: // COMMAND
          return COMMAND;
        case 2: // CONF
          return CONF;
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
    tmpMap.put(_Fields.COMMAND, new org.apache.thrift.meta_data.FieldMetaData("command", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONF, new org.apache.thrift.meta_data.FieldMetaData("conf", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "JobConfInfo")));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CommandLineJobInfo.class, metaDataMap);
  }

  public CommandLineJobInfo() {
  }

  public CommandLineJobInfo(
    String command,
    JobConfInfo conf)
  {
    this();
    this.command = command;
    this.conf = conf;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CommandLineJobInfo(CommandLineJobInfo other) {
    if (other.isSetCommand()) {
      this.command = other.command;
    }
    if (other.isSetConf()) {
      this.conf = other.conf;
    }
  }

  public CommandLineJobInfo deepCopy() {
    return new CommandLineJobInfo(this);
  }

  @Override
  public void clear() {
    this.command = null;
    this.conf = null;
  }

  public String getCommand() {
    return this.command;
  }

  public CommandLineJobInfo setCommand(String command) {
    this.command = command;
    return this;
  }

  public void unsetCommand() {
    this.command = null;
  }

  /** Returns true if field command is set (has been assigned a value) and false otherwise */
  public boolean isSetCommand() {
    return this.command != null;
  }

  public void setCommandIsSet(boolean value) {
    if (!value) {
      this.command = null;
    }
  }

  public JobConfInfo getConf() {
    return this.conf;
  }

  public CommandLineJobInfo setConf(JobConfInfo conf) {
    this.conf = conf;
    return this;
  }

  public void unsetConf() {
    this.conf = null;
  }

  /** Returns true if field conf is set (has been assigned a value) and false otherwise */
  public boolean isSetConf() {
    return this.conf != null;
  }

  public void setConfIsSet(boolean value) {
    if (!value) {
      this.conf = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COMMAND:
      if (value == null) {
        unsetCommand();
      } else {
        setCommand((String)value);
      }
      break;

    case CONF:
      if (value == null) {
        unsetConf();
      } else {
        setConf((JobConfInfo)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COMMAND:
      return getCommand();

    case CONF:
      return getConf();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COMMAND:
      return isSetCommand();
    case CONF:
      return isSetConf();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CommandLineJobInfo)
      return this.equals((CommandLineJobInfo)that);
    return false;
  }

  public boolean equals(CommandLineJobInfo that) {
    if (that == null)
      return false;

    boolean this_present_command = true && this.isSetCommand();
    boolean that_present_command = true && that.isSetCommand();
    if (this_present_command || that_present_command) {
      if (!(this_present_command && that_present_command))
        return false;
      if (!this.command.equals(that.command))
        return false;
    }

    boolean this_present_conf = true && this.isSetConf();
    boolean that_present_conf = true && that.isSetConf();
    if (this_present_conf || that_present_conf) {
      if (!(this_present_conf && that_present_conf))
        return false;
      if (!this.conf.equals(that.conf))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_command = true && (isSetCommand());
    list.add(present_command);
    if (present_command)
      list.add(command);

    boolean present_conf = true && (isSetConf());
    list.add(present_conf);
    if (present_conf)
      list.add(conf);

    return list.hashCode();
  }

  @Override
  public int compareTo(CommandLineJobInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCommand()).compareTo(other.isSetCommand());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCommand()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.command, other.command);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetConf()).compareTo(other.isSetConf());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConf()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.conf, other.conf);
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
    StringBuilder sb = new StringBuilder("CommandLineJobInfo(");
    boolean first = true;

    sb.append("command:");
    if (this.command == null) {
      sb.append("null");
    } else {
      sb.append(this.command);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("conf:");
    if (this.conf == null) {
      sb.append("null");
    } else {
      sb.append(this.conf);
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

  private static class CommandLineJobInfoStandardSchemeFactory implements SchemeFactory {
    public CommandLineJobInfoStandardScheme getScheme() {
      return new CommandLineJobInfoStandardScheme();
    }
  }

  private static class CommandLineJobInfoStandardScheme extends StandardScheme<CommandLineJobInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CommandLineJobInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COMMAND
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.command = iprot.readString();
              struct.setCommandIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CONF
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.conf = new JobConfInfo();
              struct.conf.read(iprot);
              struct.setConfIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CommandLineJobInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.command != null) {
        oprot.writeFieldBegin(COMMAND_FIELD_DESC);
        oprot.writeString(struct.command);
        oprot.writeFieldEnd();
      }
      if (struct.conf != null) {
        oprot.writeFieldBegin(CONF_FIELD_DESC);
        struct.conf.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CommandLineJobInfoTupleSchemeFactory implements SchemeFactory {
    public CommandLineJobInfoTupleScheme getScheme() {
      return new CommandLineJobInfoTupleScheme();
    }
  }

  private static class CommandLineJobInfoTupleScheme extends TupleScheme<CommandLineJobInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CommandLineJobInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetCommand()) {
        optionals.set(0);
      }
      if (struct.isSetConf()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetCommand()) {
        oprot.writeString(struct.command);
      }
      if (struct.isSetConf()) {
        struct.conf.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CommandLineJobInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.command = iprot.readString();
        struct.setCommandIsSet(true);
      }
      if (incoming.get(1)) {
        struct.conf = new JobConfInfo();
        struct.conf.read(iprot);
        struct.setConfIsSet(true);
      }
    }
  }

}

