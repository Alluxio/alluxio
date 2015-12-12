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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-12-12")
public class JobConfInfo implements org.apache.thrift.TBase<JobConfInfo, JobConfInfo._Fields>, java.io.Serializable, Cloneable, Comparable<JobConfInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("JobConfInfo");

  private static final org.apache.thrift.protocol.TField OUTPUT_FILE_FIELD_DESC = new org.apache.thrift.protocol.TField("outputFile", org.apache.thrift.protocol.TType.STRING, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new JobConfInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new JobConfInfoTupleSchemeFactory());
  }

  public String outputFile; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OUTPUT_FILE((short)1, "outputFile");

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
        case 1: // OUTPUT_FILE
          return OUTPUT_FILE;
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
    tmpMap.put(_Fields.OUTPUT_FILE, new org.apache.thrift.meta_data.FieldMetaData("outputFile", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(JobConfInfo.class, metaDataMap);
  }

  public JobConfInfo() {
  }

  public JobConfInfo(
    String outputFile)
  {
    this();
    this.outputFile = outputFile;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public JobConfInfo(JobConfInfo other) {
    if (other.isSetOutputFile()) {
      this.outputFile = other.outputFile;
    }
  }

  public JobConfInfo deepCopy() {
    return new JobConfInfo(this);
  }

  @Override
  public void clear() {
    this.outputFile = null;
  }

  public String getOutputFile() {
    return this.outputFile;
  }

  public JobConfInfo setOutputFile(String outputFile) {
    this.outputFile = outputFile;
    return this;
  }

  public void unsetOutputFile() {
    this.outputFile = null;
  }

  /** Returns true if field outputFile is set (has been assigned a value) and false otherwise */
  public boolean isSetOutputFile() {
    return this.outputFile != null;
  }

  public void setOutputFileIsSet(boolean value) {
    if (!value) {
      this.outputFile = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case OUTPUT_FILE:
      if (value == null) {
        unsetOutputFile();
      } else {
        setOutputFile((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case OUTPUT_FILE:
      return getOutputFile();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case OUTPUT_FILE:
      return isSetOutputFile();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof JobConfInfo)
      return this.equals((JobConfInfo)that);
    return false;
  }

  public boolean equals(JobConfInfo that) {
    if (that == null)
      return false;

    boolean this_present_outputFile = true && this.isSetOutputFile();
    boolean that_present_outputFile = true && that.isSetOutputFile();
    if (this_present_outputFile || that_present_outputFile) {
      if (!(this_present_outputFile && that_present_outputFile))
        return false;
      if (!this.outputFile.equals(that.outputFile))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_outputFile = true && (isSetOutputFile());
    list.add(present_outputFile);
    if (present_outputFile)
      list.add(outputFile);

    return list.hashCode();
  }

  @Override
  public int compareTo(JobConfInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetOutputFile()).compareTo(other.isSetOutputFile());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOutputFile()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.outputFile, other.outputFile);
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
    StringBuilder sb = new StringBuilder("JobConfInfo(");
    boolean first = true;

    sb.append("outputFile:");
    if (this.outputFile == null) {
      sb.append("null");
    } else {
      sb.append(this.outputFile);
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

  private static class JobConfInfoStandardSchemeFactory implements SchemeFactory {
    public JobConfInfoStandardScheme getScheme() {
      return new JobConfInfoStandardScheme();
    }
  }

  private static class JobConfInfoStandardScheme extends StandardScheme<JobConfInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, JobConfInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OUTPUT_FILE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.outputFile = iprot.readString();
              struct.setOutputFileIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, JobConfInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.outputFile != null) {
        oprot.writeFieldBegin(OUTPUT_FILE_FIELD_DESC);
        oprot.writeString(struct.outputFile);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class JobConfInfoTupleSchemeFactory implements SchemeFactory {
    public JobConfInfoTupleScheme getScheme() {
      return new JobConfInfoTupleScheme();
    }
  }

  private static class JobConfInfoTupleScheme extends TupleScheme<JobConfInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, JobConfInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetOutputFile()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetOutputFile()) {
        oprot.writeString(struct.outputFile);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, JobConfInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.outputFile = iprot.readString();
        struct.setOutputFileIsSet(true);
      }
    }
  }

}

