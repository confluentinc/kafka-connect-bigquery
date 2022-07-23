/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.wepay.kafka.connect.bigquery.convert;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to construct schema and record for Merge Field.
 */
public class MergeFieldBuilder {

    public static final String MERGE_ITERATION_FIELD_NAME = "indexInBatch";
    public static final String MERGE_BATCH_NUMBER_FIELD = "batchNumber";

    /**
     * Construct schema for Kafka Data Field
     *
     * @param mergeFieldName The configured name of Merge Field
     * @return Field of Kafka Data, with definitions of kafka batch number and insertion ID.
     */
    public static Field buildMergeField(String mergeFieldName) {

        Field iterationField = Field.of(MERGE_ITERATION_FIELD_NAME, LegacySQLTypeName.INTEGER);
        Field batchNumberField = Field.of(MERGE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER);

        return Field.newBuilder(mergeFieldName, LegacySQLTypeName.RECORD,
                        iterationField, batchNumberField)
                .setMode(Field.Mode.NULLABLE).build();
    }

    public static Map<String, Object> buildKafkaDataRecord(int batchNumber, long indexInBatch) {
        HashMap<String, Object> kafkaData = new HashMap<>();
        kafkaData.put(MERGE_ITERATION_FIELD_NAME, indexInBatch);
        kafkaData.put(MERGE_BATCH_NUMBER_FIELD, batchNumber);
        return kafkaData;
    }
}
