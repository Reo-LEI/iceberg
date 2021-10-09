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

import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.RecordWrapperTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;

public class TestStructRecordWrapper extends RecordWrapperTest {

  @Override
  protected void generateAndValidate(Schema schema, AssertMethod assertMethod) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 101L);

    InternalRecordWrapper internalRecordWrapper = new InternalRecordWrapper(schema.asStruct());
    StructRecordWrapper structRecordWrapper = new StructRecordWrapper(schema.asStruct());

    Iterator<Record> records = recordList.iterator();
    LogicalType rowType = FlinkSchemaUtil.convert(schema);
    for (int i = 0; i < numRecords; i++) {
      Assert.assertTrue("Should have more records", records.hasNext());

      StructLike record = records.next();
      RowData recordRowData = structRecordWrapper.wrap(internalRecordWrapper.wrap(record));

      TestHelpers.assertRowData(schema.asStruct(), rowType, record, recordRowData);
    }

    Assert.assertFalse("Shouldn't have more record", records.hasNext());
  }
}
