/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 */
package org.apache.spark.sql.thriftserver.cli.thrift;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class TGetQueryIdResp implements org.apache.thrift.TBase<TGetQueryIdResp, TGetQueryIdResp._Fields>, java.io.Serializable, Cloneable, Comparable<TGetQueryIdResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TGetQueryIdResp");

  private static final org.apache.thrift.protocol.TField QUERY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("queryId", org.apache.thrift.protocol.TType.STRING, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TGetQueryIdRespStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TGetQueryIdRespTupleSchemeFactory());
  }

  private String queryId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY_ID((short)1, "queryId");

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
        case 1: // QUERY_ID
          return QUERY_ID;
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
    tmpMap.put(_Fields.QUERY_ID, new org.apache.thrift.meta_data.FieldMetaData("queryId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TGetQueryIdResp.class, metaDataMap);
  }

  public TGetQueryIdResp() {
  }

  public TGetQueryIdResp(
    String queryId)
  {
    this();
    this.queryId = queryId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TGetQueryIdResp(TGetQueryIdResp other) {
    if (other.isSetQueryId()) {
      this.queryId = other.queryId;
    }
  }

  public TGetQueryIdResp deepCopy() {
    return new TGetQueryIdResp(this);
  }

  @Override
  public void clear() {
    this.queryId = null;
  }

  public String getQueryId() {
    return this.queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void unsetQueryId() {
    this.queryId = null;
  }

  /** Returns true if field queryId is set (has been assigned a value) and false otherwise */
  public boolean isSetQueryId() {
    return this.queryId != null;
  }

  public void setQueryIdIsSet(boolean value) {
    if (!value) {
      this.queryId = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case QUERY_ID:
      if (value == null) {
        unsetQueryId();
      } else {
        setQueryId((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY_ID:
      return getQueryId();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case QUERY_ID:
      return isSetQueryId();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TGetQueryIdResp)
      return this.equals((TGetQueryIdResp)that);
    return false;
  }

  public boolean equals(TGetQueryIdResp that) {
    if (that == null)
      return false;

    boolean this_present_queryId = true && this.isSetQueryId();
    boolean that_present_queryId = true && that.isSetQueryId();
    if (this_present_queryId || that_present_queryId) {
      if (!(this_present_queryId && that_present_queryId))
        return false;
      if (!this.queryId.equals(that.queryId))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_queryId = true && (isSetQueryId());
    list.add(present_queryId);
    if (present_queryId)
      list.add(queryId);

    return list.hashCode();
  }

  @Override
  public int compareTo(TGetQueryIdResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetQueryId()).compareTo(other.isSetQueryId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryId, other.queryId);
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
    StringBuilder sb = new StringBuilder("TGetQueryIdResp(");
    boolean first = true;

    sb.append("queryId:");
    if (this.queryId == null) {
      sb.append("null");
    } else {
      sb.append(this.queryId);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetQueryId()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'queryId' is unset! Struct:" + toString());
    }

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

  private static class TGetQueryIdRespStandardSchemeFactory implements SchemeFactory {
    public TGetQueryIdRespStandardScheme getScheme() {
      return new TGetQueryIdRespStandardScheme();
    }
  }

  private static class TGetQueryIdRespStandardScheme extends StandardScheme<TGetQueryIdResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TGetQueryIdResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // QUERY_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.queryId = iprot.readString();
              struct.setQueryIdIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TGetQueryIdResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.queryId != null) {
        oprot.writeFieldBegin(QUERY_ID_FIELD_DESC);
        oprot.writeString(struct.queryId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TGetQueryIdRespTupleSchemeFactory implements SchemeFactory {
    public TGetQueryIdRespTupleScheme getScheme() {
      return new TGetQueryIdRespTupleScheme();
    }
  }

  private static class TGetQueryIdRespTupleScheme extends TupleScheme<TGetQueryIdResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TGetQueryIdResp struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.queryId);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TGetQueryIdResp struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.queryId = iprot.readString();
      struct.setQueryIdIsSet(true);
    }
  }

}

