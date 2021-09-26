/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.StructRecordWrapper;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.StructCopy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Serializer;
import org.apache.iceberg.types.Serializers;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import static org.apache.iceberg.TableProperties.DELTA_WRITER_IMPL;
import static org.apache.iceberg.TableProperties.DELTA_WRITER_IMPL_DEFAULT;
import static org.apache.iceberg.TableProperties.DELTA_WRITER_IMPL_FILE;
import static org.apache.iceberg.TableProperties.DELTA_WRITER_IMPL_IN_MEMORY;
import static org.apache.iceberg.TableProperties.DELTA_WRITER_IMPL_ROCKSDB;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<RowData> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final boolean upsert;
  private final Map<String, String> properties;

  BaseDeltaTaskWriter(PartitionSpec spec,
                      FileFormat format,
                      FileAppenderFactory<RowData> appenderFactory,
                      OutputFileFactory fileFactory,
                      FileIO io,
                      long targetFileSize,
                      Map<String, String> properties,
                      Schema schema,
                      RowType flinkSchema,
                      List<Integer> equalityFieldIds,
                      boolean upsert) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize, properties);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.upsert = upsert;
    this.properties = properties;
  }

  abstract RowDataDeltaWriter route(RowData row);

  RowDataWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row);

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        if (upsert) {
          writer.delete(row);
        }
        writer.write(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break;  // UPDATE_BEFORE is not necessary for UPDATE, we do nothing to prevent delete one row twice
        }
        writer.delete(row);
        break;
      case DELETE:
        writer.delete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  protected RowDataDeltaWriter loadWriter(PartitionKey partition) {
    String impl = properties.getOrDefault(DELTA_WRITER_IMPL, DELTA_WRITER_IMPL_DEFAULT);
    switch (impl) {
      case DELTA_WRITER_IMPL_IN_MEMORY:
        throw new UnsupportedOperationException("Unsupported RowDataDeltaWriter implementation: " + impl);
      case DELTA_WRITER_IMPL_ROCKSDB: return new RocksDBDeltaWriter(partition);
      case DELTA_WRITER_IMPL_FILE: return new FileDeltaWriter(partition);
      default: throw new UnsupportedOperationException("Unknown RowDataDeltaWriter implementation: " + impl);
    }
  }

  protected interface RowDataDeltaWriter extends Cloneable {

    StructLike wrap(RowData row);

    void write(RowData row) throws IOException;

    void delete(RowData row) throws IOException;

    void deleteKey(RowData key) throws IOException;

    void close() throws IOException;

  }

  private class InMemoryDeltaWriter implements RowDataDeltaWriter {

    private InMemoryDeltaWriter(PartitionKey partition) {

    }

    @Override
    public StructLike wrap(RowData row) {
      return wrapper.wrap(row);
    }

    @Override
    public void write(RowData row) throws IOException {

    }

    @Override
    public void delete(RowData row) throws IOException {

    }

    @Override
    public void deleteKey(RowData key) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
  }

  private class RocksDBDeltaWriter implements RowDataDeltaWriter {
    private static final String ROCKSDB_DIR_OPTION = "rocksdb-dir";
    private static final String ROCKSDB_DIR_PREFIX = "iceberg-rocksdb-delta-writer-";

    private final StructRecordWrapper structRecordWrapper;
    private RollingFileWriter dataWriter;
    private RollingEqDeleteWriter eqDeleteWriter;

    private final String path;
    private final RocksDB db;
    private final DBOptions dbOptions = new DBOptions();
    private final WriteOptions writeOptions = new WriteOptions();
    private final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
    private final List<ColumnFamilyHandle> cfHandles = Lists.newArrayList();

    private final StructProjection structProjection;
    private final Serializer<StructLike> keySerializer;
    private final Serializer<StructLike> valSerializer;

    private RocksDBDeltaWriter(PartitionKey partition) {
      RocksDB.loadLibrary();

      this.structRecordWrapper = new StructRecordWrapper(schema.asStruct());
      this.dataWriter = new RollingFileWriter(partition);
      this.eqDeleteWriter = new RollingEqDeleteWriter(partition);

      String dir = properties.getOrDefault(ROCKSDB_DIR_OPTION, System.getProperty("java.io.tmpdir"));
      try {
        path = Files.createTempDirectory(Paths.get(dir), ROCKSDB_DIR_PREFIX).toString();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create the temporary directory", e);
      }

      writeOptions.setDisableWAL(true);
      dbOptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
      List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
          new ColumnFamilyDescriptor("insert-rows".getBytes(), cfOptions),
          new ColumnFamilyDescriptor("delete-rows".getBytes(), cfOptions)
      );

      try {
        this.db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, cfHandles);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }

      Preconditions.checkNotNull(schema, "Iceberg table schema cannot be null.");
      Preconditions.checkNotNull(deleteSchema, "Equality-delete schema cannot be null.");
      this.structProjection = StructProjection.create(schema, deleteSchema);
      Types.StructType keyType = deleteSchema.asStruct();
      Types.StructType valType = schema.asStruct();
      this.keySerializer = Serializers.forType(keyType);
      this.valSerializer = Serializers.forType(valType);
    }

    @Override
    public StructLike wrap(RowData row) {
      return wrapper.wrap(row);
    }

    @Override
    public void write(RowData row) throws IOException {
      ColumnFamilyHandle insertTable = cfHandles.get(1);
      byte[] key = keySerializer.serialize(StructCopy.copy(structProjection.wrap(wrap(row))));
      byte[] value = valSerializer.serialize(wrap(row));
      try {
        db.put(insertTable, writeOptions, key, value);
      } catch (RocksDBException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void delete(RowData row) throws IOException {
      ColumnFamilyHandle insertTable = cfHandles.get(1);
      ColumnFamilyHandle deleteTable = cfHandles.get(2);

      byte[] key = keySerializer.serialize(StructCopy.copy(structProjection.wrap(wrap(row))));
      byte[] value = valSerializer.serialize(wrap(row));
      try {
        db.delete(insertTable, writeOptions, key);
        db.put(deleteTable, writeOptions, key, value);
      } catch (RocksDBException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void deleteKey(RowData row) throws IOException {
      ColumnFamilyHandle insertTable = cfHandles.get(1);
      ColumnFamilyHandle deleteTable = cfHandles.get(2);

      byte[] key = keySerializer.serialize(StructCopy.copy(wrap(row)));
      try {
        db.delete(insertTable, writeOptions, key);
        db.put(deleteTable, writeOptions, key, key);
      } catch (RocksDBException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // flush all data to fs
      if (dataWriter != null) {
        ColumnFamilyHandle insertTable = cfHandles.get(1);
        try (RocksIterator iter = db.newIterator(insertTable)) {
          for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            RowData row = structRecordWrapper.wrap(RowKind.INSERT, valSerializer.deserialize(iter.value()));
            dataWriter.write(row);
          }
        } finally {
          dataWriter.close();
          dataWriter = null;
        }
      }

      if (eqDeleteWriter != null) {
        ColumnFamilyHandle deleteTable = cfHandles.get(2);
        try (RocksIterator iter = db.newIterator(deleteTable)) {
          for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            RowData row = structRecordWrapper.wrap(RowKind.DELETE, valSerializer.deserialize(iter.value()));
            eqDeleteWriter.write(row);
          }
        } finally {
          eqDeleteWriter.close();
          eqDeleteWriter = null;
        }
      }

      // close RocksDB in specific order
      cfHandles.forEach(ColumnFamilyHandle::close);
      db.close();
      dbOptions.close();
      writeOptions.close();
      cfOptions.close();

      // remove RocksDB data directory
      try {
        FileUtils.cleanDirectory(new File(path));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private class FileDeltaWriter extends BaseEqualityDeltaWriter implements RowDataDeltaWriter {

    private FileDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
    }

    @Override
    public StructLike wrap(RowData row) {
      return wrapper.wrap(row);
    }

    @Override
    protected StructLike asStructLike(RowData data) {
      return wrap(data);
    }
  }
}
