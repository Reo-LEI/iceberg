/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class RecordWrapper implements RowData {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private RowKind rowKind = RowKind.INSERT;
  private StructLike record = null;

  private Types.StructType struct;

  public RecordWrapper(Types.StructType struct) {
    this.struct = struct;
  }

  public RecordWrapper wrap(RowKind rowKind, StructLike record) {
    this.rowKind = rowKind;
    this.record = record;
    return this;
  }

  @Override
  public int getArity() {
    return record.size();
  }

  @Override
  public RowKind getRowKind() {
    return rowKind;
  }

  @Override
  public void setRowKind(RowKind kind) {
    throw new UnsupportedOperationException("Could not set row kind in the RecordWrapper because record is read-only");
  }

  @Override
  public boolean isNullAt(int pos) {
    return record.get(pos, Object.class) == null;
  }

  @Override
  public boolean getBoolean(int pos) {
    return record.get(pos, Boolean.class);
  }

  @Override
  public byte getByte(int pos) {
    return record.get(pos, ByteBuffer.class).get();
  }

  @Override
  public short getShort(int pos) {
    return record.get(pos, Integer.class).shortValue();
  }

  @Override
  public int getInt(int pos) {
    switch (struct.fields().get(pos).type().typeId()) {
      case DATE:
        return (int) ChronoUnit.DAYS.between(EPOCH_DAY, record.get(pos, LocalDate.class));
      case TIME:
        return (int) TimeUnit.NANOSECONDS.toMillis(record.get(pos, LocalTime.class).toNanoOfDay());
      default:
        return record.get(pos, Integer.class);
    }
  }

  @Override
  public long getLong(int pos) {
    return record.get(pos, Long.class);
  }

  @Override
  public float getFloat(int pos) {
    return record.get(pos, Float.class);
  }

  @Override
  public double getDouble(int pos) {
    return record.get(pos, Double.class);
  }

  @Override
  public StringData getString(int pos) {
    return StringData.fromString(String.valueOf(record.get(pos, struct.fields().get(pos).type().typeId().javaClass())));
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return DecimalData.fromBigDecimal(record.get(pos, BigDecimal.class), precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    Types.TimestampType timestampType = (Types.TimestampType) struct.fields().get(pos).type();
    if (timestampType.shouldAdjustToUTC()) {
      OffsetDateTime dateTime = (OffsetDateTime) record.get(pos, timestampType.typeId().javaClass());
      return TimestampData.fromInstant(dateTime.toInstant());
    }
    return TimestampData.fromLocalDateTime((LocalDateTime) record.get(pos, timestampType.typeId().javaClass()));
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    throw new UnsupportedOperationException("Not a supported type: " + RawValueData.class);
  }

  @Override
  public byte[] getBinary(int pos) {
    if (Type.TypeID.UUID == struct.fields().get(pos).type().typeId()) {
      UUID uuid = record.get(pos, UUID.class);
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }

    ByteBuffer buffer = record.get(pos, ByteBuffer.class);
    int from = buffer.arrayOffset() + buffer.position();
    int to = buffer.arrayOffset() + buffer.remaining();
    return Arrays.copyOfRange(buffer.array(), from, to);
  }

  @Override
  public ArrayData getArray(int pos) {
    Types.ListType listType = struct.fields().get(pos).type().asListType();
    Class<?> javaClass = listType.elementType().typeId().javaClass();

    List<?> list = record.get(pos, List.class);
    Object[] arrayData = new Object[list.size()];
    for (int i = 0; i < arrayData.length; i++) {
      arrayData[i] = javaClass.cast(list.get(i));
    }
    return new GenericArrayData(arrayData);
  }

  @Override
  public MapData getMap(int pos) {
    Types.MapType mapType = struct.fields().get(pos).type().asMapType();
    Class<?> keyClass = mapType.keyType().typeId().javaClass();
    Class<?> valueClass = mapType.valueType().typeId().javaClass();

    Map<?, ?> map = record.get(pos, Map.class);
    Map<Object, Object> mapData = Maps.newLinkedHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      mapData.put(keyClass.cast(entry.getKey()), valueClass.cast(entry.getValue()));
    }
    return new GenericMapData(mapData);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    Types.StructType structType = struct.fields().get(pos).type().asStructType();
    RecordWrapper nestedWrapper = new RecordWrapper(structType);
    return nestedWrapper.wrap(rowKind, record.get(pos, StructLike.class));
  }
}
