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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-30")
public class LineageInfo implements org.apache.thrift.TBase<LineageInfo, LineageInfo._Fields>, java.io.Serializable, Cloneable, Comparable<LineageInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LineageInfo");

  private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField INPUT_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField("inputFiles", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField OUTPUT_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField("outputFiles", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField JOB_FIELD_DESC = new org.apache.thrift.protocol.TField("job", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField CREATION_TIME_MS_FIELD_DESC = new org.apache.thrift.protocol.TField("creationTimeMs", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField PARENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("parents", org.apache.thrift.protocol.TType.LIST, (short)6);
  private static final org.apache.thrift.protocol.TField CHILDREN_FIELD_DESC = new org.apache.thrift.protocol.TField("children", org.apache.thrift.protocol.TType.LIST, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LineageInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LineageInfoTupleSchemeFactory());
  }

  public long id; // required
  public List<Long> inputFiles; // required
  public List<LineageFileInfo> outputFiles; // required
  public CommandLineJobInfo job; // required
  public long creationTimeMs; // required
  public List<Long> parents; // required
  public List<Long> children; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    INPUT_FILES((short)2, "inputFiles"),
    OUTPUT_FILES((short)3, "outputFiles"),
    JOB((short)4, "job"),
    CREATION_TIME_MS((short)5, "creationTimeMs"),
    PARENTS((short)6, "parents"),
    CHILDREN((short)7, "children");

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
        case 1: // ID
          return ID;
        case 2: // INPUT_FILES
          return INPUT_FILES;
        case 3: // OUTPUT_FILES
          return OUTPUT_FILES;
        case 4: // JOB
          return JOB;
        case 5: // CREATION_TIME_MS
          return CREATION_TIME_MS;
        case 6: // PARENTS
          return PARENTS;
        case 7: // CHILDREN
          return CHILDREN;
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
  private static final int __ID_ISSET_ID = 0;
  private static final int __CREATIONTIMEMS_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.INPUT_FILES, new org.apache.thrift.meta_data.FieldMetaData("inputFiles", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.OUTPUT_FILES, new org.apache.thrift.meta_data.FieldMetaData("outputFiles", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LineageFileInfo.class))));
    tmpMap.put(_Fields.JOB, new org.apache.thrift.meta_data.FieldMetaData("job", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, CommandLineJobInfo.class)));
    tmpMap.put(_Fields.CREATION_TIME_MS, new org.apache.thrift.meta_data.FieldMetaData("creationTimeMs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.PARENTS, new org.apache.thrift.meta_data.FieldMetaData("parents", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.CHILDREN, new org.apache.thrift.meta_data.FieldMetaData("children", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LineageInfo.class, metaDataMap);
  }

  public LineageInfo() {
  }

  public LineageInfo(
    long id,
    List<Long> inputFiles,
    List<LineageFileInfo> outputFiles,
    CommandLineJobInfo job,
    long creationTimeMs,
    List<Long> parents,
    List<Long> children)
  {
    this();
    this.id = id;
    setIdIsSet(true);
    this.inputFiles = inputFiles;
    this.outputFiles = outputFiles;
    this.job = job;
    this.creationTimeMs = creationTimeMs;
    setCreationTimeMsIsSet(true);
    this.parents = parents;
    this.children = children;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LineageInfo(LineageInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.id = other.id;
    if (other.isSetInputFiles()) {
      List<Long> __this__inputFiles = new ArrayList<Long>(other.inputFiles);
      this.inputFiles = __this__inputFiles;
    }
    if (other.isSetOutputFiles()) {
      List<LineageFileInfo> __this__outputFiles = new ArrayList<LineageFileInfo>(other.outputFiles.size());
      for (LineageFileInfo other_element : other.outputFiles) {
        __this__outputFiles.add(new LineageFileInfo(other_element));
      }
      this.outputFiles = __this__outputFiles;
    }
    if (other.isSetJob()) {
      this.job = new CommandLineJobInfo(other.job);
    }
    this.creationTimeMs = other.creationTimeMs;
    if (other.isSetParents()) {
      List<Long> __this__parents = new ArrayList<Long>(other.parents);
      this.parents = __this__parents;
    }
    if (other.isSetChildren()) {
      List<Long> __this__children = new ArrayList<Long>(other.children);
      this.children = __this__children;
    }
  }

  public LineageInfo deepCopy() {
    return new LineageInfo(this);
  }

  @Override
  public void clear() {
    setIdIsSet(false);
    this.id = 0;
    this.inputFiles = null;
    this.outputFiles = null;
    this.job = null;
    setCreationTimeMsIsSet(false);
    this.creationTimeMs = 0;
    this.parents = null;
    this.children = null;
  }

  public long getId() {
    return this.id;
  }

  public LineageInfo setId(long id) {
    this.id = id;
    setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ID_ISSET_ID);
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean isSetId() {
    return EncodingUtils.testBit(__isset_bitfield, __ID_ISSET_ID);
  }

  public void setIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ID_ISSET_ID, value);
  }

  public int getInputFilesSize() {
    return (this.inputFiles == null) ? 0 : this.inputFiles.size();
  }

  public java.util.Iterator<Long> getInputFilesIterator() {
    return (this.inputFiles == null) ? null : this.inputFiles.iterator();
  }

  public void addToInputFiles(long elem) {
    if (this.inputFiles == null) {
      this.inputFiles = new ArrayList<Long>();
    }
    this.inputFiles.add(elem);
  }

  public List<Long> getInputFiles() {
    return this.inputFiles;
  }

  public LineageInfo setInputFiles(List<Long> inputFiles) {
    this.inputFiles = inputFiles;
    return this;
  }

  public void unsetInputFiles() {
    this.inputFiles = null;
  }

  /** Returns true if field inputFiles is set (has been assigned a value) and false otherwise */
  public boolean isSetInputFiles() {
    return this.inputFiles != null;
  }

  public void setInputFilesIsSet(boolean value) {
    if (!value) {
      this.inputFiles = null;
    }
  }

  public int getOutputFilesSize() {
    return (this.outputFiles == null) ? 0 : this.outputFiles.size();
  }

  public java.util.Iterator<LineageFileInfo> getOutputFilesIterator() {
    return (this.outputFiles == null) ? null : this.outputFiles.iterator();
  }

  public void addToOutputFiles(LineageFileInfo elem) {
    if (this.outputFiles == null) {
      this.outputFiles = new ArrayList<LineageFileInfo>();
    }
    this.outputFiles.add(elem);
  }

  public List<LineageFileInfo> getOutputFiles() {
    return this.outputFiles;
  }

  public LineageInfo setOutputFiles(List<LineageFileInfo> outputFiles) {
    this.outputFiles = outputFiles;
    return this;
  }

  public void unsetOutputFiles() {
    this.outputFiles = null;
  }

  /** Returns true if field outputFiles is set (has been assigned a value) and false otherwise */
  public boolean isSetOutputFiles() {
    return this.outputFiles != null;
  }

  public void setOutputFilesIsSet(boolean value) {
    if (!value) {
      this.outputFiles = null;
    }
  }

  public CommandLineJobInfo getJob() {
    return this.job;
  }

  public LineageInfo setJob(CommandLineJobInfo job) {
    this.job = job;
    return this;
  }

  public void unsetJob() {
    this.job = null;
  }

  /** Returns true if field job is set (has been assigned a value) and false otherwise */
  public boolean isSetJob() {
    return this.job != null;
  }

  public void setJobIsSet(boolean value) {
    if (!value) {
      this.job = null;
    }
  }

  public long getCreationTimeMs() {
    return this.creationTimeMs;
  }

  public LineageInfo setCreationTimeMs(long creationTimeMs) {
    this.creationTimeMs = creationTimeMs;
    setCreationTimeMsIsSet(true);
    return this;
  }

  public void unsetCreationTimeMs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __CREATIONTIMEMS_ISSET_ID);
  }

  /** Returns true if field creationTimeMs is set (has been assigned a value) and false otherwise */
  public boolean isSetCreationTimeMs() {
    return EncodingUtils.testBit(__isset_bitfield, __CREATIONTIMEMS_ISSET_ID);
  }

  public void setCreationTimeMsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __CREATIONTIMEMS_ISSET_ID, value);
  }

  public int getParentsSize() {
    return (this.parents == null) ? 0 : this.parents.size();
  }

  public java.util.Iterator<Long> getParentsIterator() {
    return (this.parents == null) ? null : this.parents.iterator();
  }

  public void addToParents(long elem) {
    if (this.parents == null) {
      this.parents = new ArrayList<Long>();
    }
    this.parents.add(elem);
  }

  public List<Long> getParents() {
    return this.parents;
  }

  public LineageInfo setParents(List<Long> parents) {
    this.parents = parents;
    return this;
  }

  public void unsetParents() {
    this.parents = null;
  }

  /** Returns true if field parents is set (has been assigned a value) and false otherwise */
  public boolean isSetParents() {
    return this.parents != null;
  }

  public void setParentsIsSet(boolean value) {
    if (!value) {
      this.parents = null;
    }
  }

  public int getChildrenSize() {
    return (this.children == null) ? 0 : this.children.size();
  }

  public java.util.Iterator<Long> getChildrenIterator() {
    return (this.children == null) ? null : this.children.iterator();
  }

  public void addToChildren(long elem) {
    if (this.children == null) {
      this.children = new ArrayList<Long>();
    }
    this.children.add(elem);
  }

  public List<Long> getChildren() {
    return this.children;
  }

  public LineageInfo setChildren(List<Long> children) {
    this.children = children;
    return this;
  }

  public void unsetChildren() {
    this.children = null;
  }

  /** Returns true if field children is set (has been assigned a value) and false otherwise */
  public boolean isSetChildren() {
    return this.children != null;
  }

  public void setChildrenIsSet(boolean value) {
    if (!value) {
      this.children = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((Long)value);
      }
      break;

    case INPUT_FILES:
      if (value == null) {
        unsetInputFiles();
      } else {
        setInputFiles((List<Long>)value);
      }
      break;

    case OUTPUT_FILES:
      if (value == null) {
        unsetOutputFiles();
      } else {
        setOutputFiles((List<LineageFileInfo>)value);
      }
      break;

    case JOB:
      if (value == null) {
        unsetJob();
      } else {
        setJob((CommandLineJobInfo)value);
      }
      break;

    case CREATION_TIME_MS:
      if (value == null) {
        unsetCreationTimeMs();
      } else {
        setCreationTimeMs((Long)value);
      }
      break;

    case PARENTS:
      if (value == null) {
        unsetParents();
      } else {
        setParents((List<Long>)value);
      }
      break;

    case CHILDREN:
      if (value == null) {
        unsetChildren();
      } else {
        setChildren((List<Long>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ID:
      return Long.valueOf(getId());

    case INPUT_FILES:
      return getInputFiles();

    case OUTPUT_FILES:
      return getOutputFiles();

    case JOB:
      return getJob();

    case CREATION_TIME_MS:
      return Long.valueOf(getCreationTimeMs());

    case PARENTS:
      return getParents();

    case CHILDREN:
      return getChildren();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ID:
      return isSetId();
    case INPUT_FILES:
      return isSetInputFiles();
    case OUTPUT_FILES:
      return isSetOutputFiles();
    case JOB:
      return isSetJob();
    case CREATION_TIME_MS:
      return isSetCreationTimeMs();
    case PARENTS:
      return isSetParents();
    case CHILDREN:
      return isSetChildren();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LineageInfo)
      return this.equals((LineageInfo)that);
    return false;
  }

  public boolean equals(LineageInfo that) {
    if (that == null)
      return false;

    boolean this_present_id = true;
    boolean that_present_id = true;
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (this.id != that.id)
        return false;
    }

    boolean this_present_inputFiles = true && this.isSetInputFiles();
    boolean that_present_inputFiles = true && that.isSetInputFiles();
    if (this_present_inputFiles || that_present_inputFiles) {
      if (!(this_present_inputFiles && that_present_inputFiles))
        return false;
      if (!this.inputFiles.equals(that.inputFiles))
        return false;
    }

    boolean this_present_outputFiles = true && this.isSetOutputFiles();
    boolean that_present_outputFiles = true && that.isSetOutputFiles();
    if (this_present_outputFiles || that_present_outputFiles) {
      if (!(this_present_outputFiles && that_present_outputFiles))
        return false;
      if (!this.outputFiles.equals(that.outputFiles))
        return false;
    }

    boolean this_present_job = true && this.isSetJob();
    boolean that_present_job = true && that.isSetJob();
    if (this_present_job || that_present_job) {
      if (!(this_present_job && that_present_job))
        return false;
      if (!this.job.equals(that.job))
        return false;
    }

    boolean this_present_creationTimeMs = true;
    boolean that_present_creationTimeMs = true;
    if (this_present_creationTimeMs || that_present_creationTimeMs) {
      if (!(this_present_creationTimeMs && that_present_creationTimeMs))
        return false;
      if (this.creationTimeMs != that.creationTimeMs)
        return false;
    }

    boolean this_present_parents = true && this.isSetParents();
    boolean that_present_parents = true && that.isSetParents();
    if (this_present_parents || that_present_parents) {
      if (!(this_present_parents && that_present_parents))
        return false;
      if (!this.parents.equals(that.parents))
        return false;
    }

    boolean this_present_children = true && this.isSetChildren();
    boolean that_present_children = true && that.isSetChildren();
    if (this_present_children || that_present_children) {
      if (!(this_present_children && that_present_children))
        return false;
      if (!this.children.equals(that.children))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_id = true;
    list.add(present_id);
    if (present_id)
      list.add(id);

    boolean present_inputFiles = true && (isSetInputFiles());
    list.add(present_inputFiles);
    if (present_inputFiles)
      list.add(inputFiles);

    boolean present_outputFiles = true && (isSetOutputFiles());
    list.add(present_outputFiles);
    if (present_outputFiles)
      list.add(outputFiles);

    boolean present_job = true && (isSetJob());
    list.add(present_job);
    if (present_job)
      list.add(job);

    boolean present_creationTimeMs = true;
    list.add(present_creationTimeMs);
    if (present_creationTimeMs)
      list.add(creationTimeMs);

    boolean present_parents = true && (isSetParents());
    list.add(present_parents);
    if (present_parents)
      list.add(parents);

    boolean present_children = true && (isSetChildren());
    list.add(present_children);
    if (present_children)
      list.add(children);

    return list.hashCode();
  }

  @Override
  public int compareTo(LineageInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetId()).compareTo(other.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, other.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetInputFiles()).compareTo(other.isSetInputFiles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInputFiles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.inputFiles, other.inputFiles);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOutputFiles()).compareTo(other.isSetOutputFiles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOutputFiles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.outputFiles, other.outputFiles);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetJob()).compareTo(other.isSetJob());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetJob()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.job, other.job);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCreationTimeMs()).compareTo(other.isSetCreationTimeMs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCreationTimeMs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.creationTimeMs, other.creationTimeMs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetParents()).compareTo(other.isSetParents());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParents()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.parents, other.parents);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetChildren()).compareTo(other.isSetChildren());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetChildren()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.children, other.children);
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
    StringBuilder sb = new StringBuilder("LineageInfo(");
    boolean first = true;

    sb.append("id:");
    sb.append(this.id);
    first = false;
    if (!first) sb.append(", ");
    sb.append("inputFiles:");
    if (this.inputFiles == null) {
      sb.append("null");
    } else {
      sb.append(this.inputFiles);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("outputFiles:");
    if (this.outputFiles == null) {
      sb.append("null");
    } else {
      sb.append(this.outputFiles);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("job:");
    if (this.job == null) {
      sb.append("null");
    } else {
      sb.append(this.job);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("creationTimeMs:");
    sb.append(this.creationTimeMs);
    first = false;
    if (!first) sb.append(", ");
    sb.append("parents:");
    if (this.parents == null) {
      sb.append("null");
    } else {
      sb.append(this.parents);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("children:");
    if (this.children == null) {
      sb.append("null");
    } else {
      sb.append(this.children);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (job != null) {
      job.validate();
    }
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

  private static class LineageInfoStandardSchemeFactory implements SchemeFactory {
    public LineageInfoStandardScheme getScheme() {
      return new LineageInfoStandardScheme();
    }
  }

  private static class LineageInfoStandardScheme extends StandardScheme<LineageInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LineageInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.id = iprot.readI64();
              struct.setIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // INPUT_FILES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list48 = iprot.readListBegin();
                struct.inputFiles = new ArrayList<Long>(_list48.size);
                long _elem49;
                for (int _i50 = 0; _i50 < _list48.size; ++_i50)
                {
                  _elem49 = iprot.readI64();
                  struct.inputFiles.add(_elem49);
                }
                iprot.readListEnd();
              }
              struct.setInputFilesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // OUTPUT_FILES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list51 = iprot.readListBegin();
                struct.outputFiles = new ArrayList<LineageFileInfo>(_list51.size);
                LineageFileInfo _elem52;
                for (int _i53 = 0; _i53 < _list51.size; ++_i53)
                {
                  _elem52 = new LineageFileInfo();
                  _elem52.read(iprot);
                  struct.outputFiles.add(_elem52);
                }
                iprot.readListEnd();
              }
              struct.setOutputFilesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // JOB
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.job = new CommandLineJobInfo();
              struct.job.read(iprot);
              struct.setJobIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // CREATION_TIME_MS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.creationTimeMs = iprot.readI64();
              struct.setCreationTimeMsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // PARENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list54 = iprot.readListBegin();
                struct.parents = new ArrayList<Long>(_list54.size);
                long _elem55;
                for (int _i56 = 0; _i56 < _list54.size; ++_i56)
                {
                  _elem55 = iprot.readI64();
                  struct.parents.add(_elem55);
                }
                iprot.readListEnd();
              }
              struct.setParentsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // CHILDREN
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list57 = iprot.readListBegin();
                struct.children = new ArrayList<Long>(_list57.size);
                long _elem58;
                for (int _i59 = 0; _i59 < _list57.size; ++_i59)
                {
                  _elem58 = iprot.readI64();
                  struct.children.add(_elem58);
                }
                iprot.readListEnd();
              }
              struct.setChildrenIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LineageInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeI64(struct.id);
      oprot.writeFieldEnd();
      if (struct.inputFiles != null) {
        oprot.writeFieldBegin(INPUT_FILES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.inputFiles.size()));
          for (long _iter60 : struct.inputFiles)
          {
            oprot.writeI64(_iter60);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.outputFiles != null) {
        oprot.writeFieldBegin(OUTPUT_FILES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.outputFiles.size()));
          for (LineageFileInfo _iter61 : struct.outputFiles)
          {
            _iter61.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.job != null) {
        oprot.writeFieldBegin(JOB_FIELD_DESC);
        struct.job.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(CREATION_TIME_MS_FIELD_DESC);
      oprot.writeI64(struct.creationTimeMs);
      oprot.writeFieldEnd();
      if (struct.parents != null) {
        oprot.writeFieldBegin(PARENTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.parents.size()));
          for (long _iter62 : struct.parents)
          {
            oprot.writeI64(_iter62);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.children != null) {
        oprot.writeFieldBegin(CHILDREN_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.children.size()));
          for (long _iter63 : struct.children)
          {
            oprot.writeI64(_iter63);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LineageInfoTupleSchemeFactory implements SchemeFactory {
    public LineageInfoTupleScheme getScheme() {
      return new LineageInfoTupleScheme();
    }
  }

  private static class LineageInfoTupleScheme extends TupleScheme<LineageInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LineageInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetId()) {
        optionals.set(0);
      }
      if (struct.isSetInputFiles()) {
        optionals.set(1);
      }
      if (struct.isSetOutputFiles()) {
        optionals.set(2);
      }
      if (struct.isSetJob()) {
        optionals.set(3);
      }
      if (struct.isSetCreationTimeMs()) {
        optionals.set(4);
      }
      if (struct.isSetParents()) {
        optionals.set(5);
      }
      if (struct.isSetChildren()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetId()) {
        oprot.writeI64(struct.id);
      }
      if (struct.isSetInputFiles()) {
        {
          oprot.writeI32(struct.inputFiles.size());
          for (long _iter64 : struct.inputFiles)
          {
            oprot.writeI64(_iter64);
          }
        }
      }
      if (struct.isSetOutputFiles()) {
        {
          oprot.writeI32(struct.outputFiles.size());
          for (LineageFileInfo _iter65 : struct.outputFiles)
          {
            _iter65.write(oprot);
          }
        }
      }
      if (struct.isSetJob()) {
        struct.job.write(oprot);
      }
      if (struct.isSetCreationTimeMs()) {
        oprot.writeI64(struct.creationTimeMs);
      }
      if (struct.isSetParents()) {
        {
          oprot.writeI32(struct.parents.size());
          for (long _iter66 : struct.parents)
          {
            oprot.writeI64(_iter66);
          }
        }
      }
      if (struct.isSetChildren()) {
        {
          oprot.writeI32(struct.children.size());
          for (long _iter67 : struct.children)
          {
            oprot.writeI64(_iter67);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LineageInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.id = iprot.readI64();
        struct.setIdIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list68 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.inputFiles = new ArrayList<Long>(_list68.size);
          long _elem69;
          for (int _i70 = 0; _i70 < _list68.size; ++_i70)
          {
            _elem69 = iprot.readI64();
            struct.inputFiles.add(_elem69);
          }
        }
        struct.setInputFilesIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list71 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.outputFiles = new ArrayList<LineageFileInfo>(_list71.size);
          LineageFileInfo _elem72;
          for (int _i73 = 0; _i73 < _list71.size; ++_i73)
          {
            _elem72 = new LineageFileInfo();
            _elem72.read(iprot);
            struct.outputFiles.add(_elem72);
          }
        }
        struct.setOutputFilesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.job = new CommandLineJobInfo();
        struct.job.read(iprot);
        struct.setJobIsSet(true);
      }
      if (incoming.get(4)) {
        struct.creationTimeMs = iprot.readI64();
        struct.setCreationTimeMsIsSet(true);
      }
      if (incoming.get(5)) {
        {
          org.apache.thrift.protocol.TList _list74 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.parents = new ArrayList<Long>(_list74.size);
          long _elem75;
          for (int _i76 = 0; _i76 < _list74.size; ++_i76)
          {
            _elem75 = iprot.readI64();
            struct.parents.add(_elem75);
          }
        }
        struct.setParentsIsSet(true);
      }
      if (incoming.get(6)) {
        {
          org.apache.thrift.protocol.TList _list77 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.children = new ArrayList<Long>(_list77.size);
          long _elem78;
          for (int _i79 = 0; _i79 < _list77.size; ++_i79)
          {
            _elem78 = iprot.readI64();
            struct.children.add(_elem78);
          }
        }
        struct.setChildrenIsSet(true);
      }
    }
  }

}

