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
public class TGetOperationStatusReq implements org.apache.thrift.TBase<TGetOperationStatusReq, TGetOperationStatusReq._Fields>, java.io.Serializable, Cloneable, Comparable<TGetOperationStatusReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TGetOperationStatusReq");

  private static final org.apache.thrift.protocol.TField OPERATION_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("operationHandle", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField GET_PROGRESS_UPDATE_FIELD_DESC = new org.apache.thrift.protocol.TField("getProgressUpdate", org.apache.thrift.protocol.TType.BOOL, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TGetOperationStatusReqStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TGetOperationStatusReqTupleSchemeFactory());
  }

  private TOperationHandle operationHandle; // required
  private boolean getProgressUpdate; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OPERATION_HANDLE((short)1, "operationHandle"),
    GET_PROGRESS_UPDATE((short)2, "getProgressUpdate");

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
        case 1: // OPERATION_HANDLE
          return OPERATION_HANDLE;
        case 2: // GET_PROGRESS_UPDATE
          return GET_PROGRESS_UPDATE;
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
  private static final int __GETPROGRESSUPDATE_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.GET_PROGRESS_UPDATE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.OPERATION_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("operationHandle", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TOperationHandle.class)));
    tmpMap.put(_Fields.GET_PROGRESS_UPDATE, new org.apache.thrift.meta_data.FieldMetaData("getProgressUpdate", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TGetOperationStatusReq.class, metaDataMap);
  }

  public TGetOperationStatusReq() {
  }

  public TGetOperationStatusReq(
    TOperationHandle operationHandle)
  {
    this();
    this.operationHandle = operationHandle;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TGetOperationStatusReq(TGetOperationStatusReq other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetOperationHandle()) {
      this.operationHandle = new TOperationHandle(other.operationHandle);
    }
    this.getProgressUpdate = other.getProgressUpdate;
  }

  public TGetOperationStatusReq deepCopy() {
    return new TGetOperationStatusReq(this);
  }

  @Override
  public void clear() {
    this.operationHandle = null;
    setGetProgressUpdateIsSet(false);
    this.getProgressUpdate = false;
  }

  public TOperationHandle getOperationHandle() {
    return this.operationHandle;
  }

  public void setOperationHandle(TOperationHandle operationHandle) {
    this.operationHandle = operationHandle;
  }

  public void unsetOperationHandle() {
    this.operationHandle = null;
  }

  /** Returns true if field operationHandle is set (has been assigned a value) and false otherwise */
  public boolean isSetOperationHandle() {
    return this.operationHandle != null;
  }

  public void setOperationHandleIsSet(boolean value) {
    if (!value) {
      this.operationHandle = null;
    }
  }

  public boolean isGetProgressUpdate() {
    return this.getProgressUpdate;
  }

  public void setGetProgressUpdate(boolean getProgressUpdate) {
    this.getProgressUpdate = getProgressUpdate;
    setGetProgressUpdateIsSet(true);
  }

  public void unsetGetProgressUpdate() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __GETPROGRESSUPDATE_ISSET_ID);
  }

  /** Returns true if field getProgressUpdate is set (has been assigned a value) and false otherwise */
  public boolean isSetGetProgressUpdate() {
    return EncodingUtils.testBit(__isset_bitfield, __GETPROGRESSUPDATE_ISSET_ID);
  }

  public void setGetProgressUpdateIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __GETPROGRESSUPDATE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case OPERATION_HANDLE:
      if (value == null) {
        unsetOperationHandle();
      } else {
        setOperationHandle((TOperationHandle)value);
      }
      break;

    case GET_PROGRESS_UPDATE:
      if (value == null) {
        unsetGetProgressUpdate();
      } else {
        setGetProgressUpdate((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case OPERATION_HANDLE:
      return getOperationHandle();

    case GET_PROGRESS_UPDATE:
      return isGetProgressUpdate();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case OPERATION_HANDLE:
      return isSetOperationHandle();
    case GET_PROGRESS_UPDATE:
      return isSetGetProgressUpdate();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TGetOperationStatusReq)
      return this.equals((TGetOperationStatusReq)that);
    return false;
  }

  public boolean equals(TGetOperationStatusReq that) {
    if (that == null)
      return false;

    boolean this_present_operationHandle = true && this.isSetOperationHandle();
    boolean that_present_operationHandle = true && that.isSetOperationHandle();
    if (this_present_operationHandle || that_present_operationHandle) {
      if (!(this_present_operationHandle && that_present_operationHandle))
        return false;
      if (!this.operationHandle.equals(that.operationHandle))
        return false;
    }

    boolean this_present_getProgressUpdate = true && this.isSetGetProgressUpdate();
    boolean that_present_getProgressUpdate = true && that.isSetGetProgressUpdate();
    if (this_present_getProgressUpdate || that_present_getProgressUpdate) {
      if (!(this_present_getProgressUpdate && that_present_getProgressUpdate))
        return false;
      if (this.getProgressUpdate != that.getProgressUpdate)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_operationHandle = true && (isSetOperationHandle());
    list.add(present_operationHandle);
    if (present_operationHandle)
      list.add(operationHandle);

    boolean present_getProgressUpdate = true && (isSetGetProgressUpdate());
    list.add(present_getProgressUpdate);
    if (present_getProgressUpdate)
      list.add(getProgressUpdate);

    return list.hashCode();
  }

  @Override
  public int compareTo(TGetOperationStatusReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetOperationHandle()).compareTo(other.isSetOperationHandle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperationHandle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operationHandle, other.operationHandle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetGetProgressUpdate()).compareTo(other.isSetGetProgressUpdate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGetProgressUpdate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.getProgressUpdate, other.getProgressUpdate);
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
    StringBuilder sb = new StringBuilder("TGetOperationStatusReq(");
    boolean first = true;

    sb.append("operationHandle:");
    if (this.operationHandle == null) {
      sb.append("null");
    } else {
      sb.append(this.operationHandle);
    }
    first = false;
    if (isSetGetProgressUpdate()) {
      if (!first) sb.append(", ");
      sb.append("getProgressUpdate:");
      sb.append(this.getProgressUpdate);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetOperationHandle()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'operationHandle' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (operationHandle != null) {
      operationHandle.validate();
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

  private static class TGetOperationStatusReqStandardSchemeFactory implements SchemeFactory {
    public TGetOperationStatusReqStandardScheme getScheme() {
      return new TGetOperationStatusReqStandardScheme();
    }
  }

  private static class TGetOperationStatusReqStandardScheme extends StandardScheme<TGetOperationStatusReq> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TGetOperationStatusReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OPERATION_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.operationHandle = new TOperationHandle();
              struct.operationHandle.read(iprot);
              struct.setOperationHandleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // GET_PROGRESS_UPDATE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.getProgressUpdate = iprot.readBool();
              struct.setGetProgressUpdateIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TGetOperationStatusReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.operationHandle != null) {
        oprot.writeFieldBegin(OPERATION_HANDLE_FIELD_DESC);
        struct.operationHandle.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.isSetGetProgressUpdate()) {
        oprot.writeFieldBegin(GET_PROGRESS_UPDATE_FIELD_DESC);
        oprot.writeBool(struct.getProgressUpdate);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TGetOperationStatusReqTupleSchemeFactory implements SchemeFactory {
    public TGetOperationStatusReqTupleScheme getScheme() {
      return new TGetOperationStatusReqTupleScheme();
    }
  }

  private static class TGetOperationStatusReqTupleScheme extends TupleScheme<TGetOperationStatusReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TGetOperationStatusReq struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.operationHandle.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetGetProgressUpdate()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetGetProgressUpdate()) {
        oprot.writeBool(struct.getProgressUpdate);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TGetOperationStatusReq struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.operationHandle = new TOperationHandle();
      struct.operationHandle.read(iprot);
      struct.setOperationHandleIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.getProgressUpdate = iprot.readBool();
        struct.setGetProgressUpdateIsSet(true);
      }
    }
  }

}

