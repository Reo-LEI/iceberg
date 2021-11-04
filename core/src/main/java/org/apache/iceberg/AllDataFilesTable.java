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

package org.apache.iceberg;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ThreadPools;

/**
 * A {@link Table} implementation that exposes a table's valid data files as rows.
 * <p>
 * A valid data file is one that is readable from any snapshot currently tracked by the table.
 * <p>
 * This table may return duplicate rows.
 */
public class AllDataFilesTable extends BaseMetadataTable {

  AllDataFilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".all_data_files");
  }

  AllDataFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllDataFilesTableScan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field (id 102)
      return TypeUtil.selectNot(schema, Sets.newHashSet(102));
    } else {
      return schema;
    }
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_DATA_FILES;
  }

  public static class AllDataFilesTableScan extends BaseAllMetadataTableScan {
    private final Schema fileSchema;

    AllDataFilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
    }

    private AllDataFilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                                  TableScanContext context) {
      super(ops, table, schema, context);
      this.fileSchema = fileSchema;
    }

    @Override
    protected String tableType() {
      return MetadataTableType.ALL_DATA_FILES.name();
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new AllDataFilesTableScan(ops, table, schema, fileSchema, context);
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    public long targetSplitSize() {
      return tableOps().current().propertyAsLong(
          TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter,
        boolean ignoreResiduals, boolean caseSensitive, boolean colStats) {
      CloseableIterable<ManifestFile> manifests = allDataManifestFiles(ops.current().snapshots());
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      return CloseableIterable.transform(manifests, manifest ->
          new DataFilesTable.ManifestReadTask(ops.io(), manifest, schema(), schemaString, specString, residuals));
    }
  }

  private static CloseableIterable<ManifestFile> allDataManifestFiles(List<Snapshot> snapshots) {
    try (CloseableIterable<ManifestFile> iterable = CloseableIterable.combine(
        Iterables.transform(snapshots, snapshot -> CloseableIterable.withNoopClose(snapshot.dataManifests())),
        ThreadPools.getWorkerPool(), ThreadPools.WORKER_THREAD_POOL_SIZE)) {
      return CloseableIterable.withNoopClose(Sets.newHashSet(iterable));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close parallel iterable");
    }
  }
}
