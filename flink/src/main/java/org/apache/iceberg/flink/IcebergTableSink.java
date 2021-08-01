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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.UPSERT_MODE_ENABLED;
import static org.apache.iceberg.TableProperties.UPSERT_MODE_ENABLED_DEFAULT;

public class IcebergTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private static final ConfigOption<String> SNK_UID_PREFIX =
      ConfigOptions.key("sink.uid.prefix")
          .stringType()
          .defaultValue(null)
          .withDescription("Iceberg sink operators uid prefix, default is null.");

  private final TableLoader tableLoader;
  private final TableSchema tableSchema;
  private final Map<String, String> tableProperties;

  private boolean overwrite = false;

  private IcebergTableSink(IcebergTableSink toCopy) {
    this.tableLoader = toCopy.tableLoader;
    this.tableSchema = toCopy.tableSchema;
    this.tableProperties = toCopy.tableProperties;
    this.overwrite = toCopy.overwrite;
  }

  public IcebergTableSink(TableLoader tableLoader, TableSchema tableSchema, Map<String, String> tableProperties) {
    this.tableLoader = tableLoader;
    this.tableSchema = tableSchema;
    this.tableProperties = tableProperties;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    Preconditions.checkState(
        !overwrite || context.isBounded(),
        "Unbounded data stream doesn't support overwrite operation.");

    List<String> equalityColumns = tableSchema.getPrimaryKey()
        .map(UniqueConstraint::getColumns)
        .orElseGet(ImmutableList::of);

    Configuration config = new Configuration();
    tableProperties.forEach(config::setString);

    return (DataStreamSinkProvider) dataStream ->  {
      // For CDC case in FlinkSQL, change log will be rebalanced(default partition strategy) distributed to Filter opr
      // when set job default parallelism greater than 1. That will make change log data disorder and produce a wrong
      // result for iceberg(e.g. +U comes before -U). Here try to specific the Filter opr parallelism same as it's
      // input to keep Filter chaining it's input and avoid rebalance.
      Transformation<?> forwardOpr = dataStream.getTransformation();
      if (forwardOpr.getName().equals("Filter") && forwardOpr.getInputs().size() == 1) {
        forwardOpr.setParallelism(forwardOpr.getInputs().get(0).getParallelism());
      }
      return FlinkSink.forRowData(dataStream)
          .tableLoader(tableLoader)
          .tableSchema(tableSchema)
          .equalityFieldColumns(equalityColumns)
          .upsert(PropertyUtil.propertyAsBoolean(tableProperties, UPSERT_MODE_ENABLED, UPSERT_MODE_ENABLED_DEFAULT))
          .uidPrefix(config.getString(SNK_UID_PREFIX))
          .overwrite(overwrite)
          .build();
    };
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    // The flink's PartitionFanoutWriter will handle the static partition write policy automatically.
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : requestedMode.getContainedKinds()) {
      builder.addContainedKind(kind);
    }
    return builder.build();
  }

  @Override
  public DynamicTableSink copy() {
    return new IcebergTableSink(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table sink";
  }

  @Override
  public void applyOverwrite(boolean newOverwrite) {
    this.overwrite = newOverwrite;
  }
}
