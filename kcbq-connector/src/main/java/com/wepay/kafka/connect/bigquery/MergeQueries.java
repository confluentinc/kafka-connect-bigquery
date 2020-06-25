package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 WePay, Inc.
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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.destTable;
import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

public class MergeQueries {
  public static final String INTERMEDIATE_TABLE_KEY_FIELD_NAME = "key";
  public static final String INTERMEDIATE_TABLE_VALUE_FIELD_NAME = "value";
  public static final String INTERMEDIATE_TABLE_ITERATION_FIELD_NAME = "i";
  public static final String INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME = "partitionTime";
  public static final String INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD = "batchNumber";

  private static final Logger logger = LoggerFactory.getLogger(MergeQueries.class);

  private final String keyFieldName;
  private final boolean insertPartitionTime;
  private final boolean upsertEnabled;
  private final boolean deleteEnabled;
  private final MergeBatches mergeBatches;
  private final KCBQThreadPoolExecutor executor;
  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final SinkTaskContext context;

  public MergeQueries(BigQuerySinkTaskConfig config,
                      MergeBatches mergeBatches,
                      KCBQThreadPoolExecutor executor,
                      BigQuery bigQuery,
                      SchemaManager schemaManager,
                      SinkTaskContext context) {
    this(
      config.getKafkaKeyFieldName().orElseThrow(() ->
          new ConnectException("Kafka key field must be configured when upsert/delete is enabled")
      ),
      config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG),
      config.getBoolean(config.UPSERT_ENABLED_CONFIG),
      config.getBoolean(config.DELETE_ENABLED_CONFIG),
      mergeBatches,
      executor,
      bigQuery,
      schemaManager,
      context
    );
  }

  @VisibleForTesting
  MergeQueries(String keyFieldName,
               boolean insertPartitionTime,
               boolean upsertEnabled,
               boolean deleteEnabled,
               MergeBatches mergeBatches,
               KCBQThreadPoolExecutor executor,
               BigQuery bigQuery,
               SchemaManager schemaManager,
               SinkTaskContext context) {
    this.keyFieldName = keyFieldName;
    this.insertPartitionTime = insertPartitionTime;
    this.upsertEnabled = upsertEnabled;
    this.deleteEnabled = deleteEnabled;
    this.mergeBatches = mergeBatches;
    this.executor = executor;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.context = context;
  }

  public void mergeFlushAll() {
    logger.debug("Triggering merge flush for all tables");
    mergeBatches.intermediateTables().forEach(this::mergeFlush);
  }

  public void mergeFlush(TableId intermediateTable) {
    final TableId destinationTable = mergeBatches.destinationTableFor(intermediateTable);
    final int batchNumber = mergeBatches.incrementBatch(intermediateTable);
    logger.trace("Triggering merge flush from {} to {} for batch {}",
        intTable(intermediateTable), destTable(destinationTable), batchNumber);

    executor.execute(() -> {
      try {
        mergeFlush(intermediateTable, destinationTable, batchNumber);
      } catch (InterruptedException e) {
        throw new ExpectedInterruptException(String.format(
            "Interrupted while performing merge flush of batch %d from %s to %s",
            batchNumber, intTable(intermediateTable), destTable(destinationTable)));
      }
    });
  }

  private void mergeFlush(
      TableId intermediateTable, TableId destinationTable, int batchNumber
  ) throws InterruptedException{
    // If there are rows to flush in this batch, flush them
    if (mergeBatches.prepareToFlush(intermediateTable, batchNumber)) {
      logger.debug("Running merge query on batch {} from {}",
          batchNumber, intTable(intermediateTable));
      String mergeFlushQuery = mergeFlushQuery(intermediateTable, destinationTable, batchNumber);
      logger.trace(mergeFlushQuery);
      bigQuery.query(QueryJobConfiguration.of(mergeFlushQuery));
      logger.trace("Merge from {} to {} completed",
          intTable(intermediateTable), destTable(destinationTable));

      logger.debug("Recording flush success for batch {} from {}",
          batchNumber, intTable(intermediateTable));
      mergeBatches.recordSuccessfulFlush(intermediateTable, batchNumber);

      // Commit those offsets ASAP
      context.requestCommit();

      logger.info("Completed merge flush of batch {} from {} to {}",
          batchNumber, intTable(intermediateTable), destTable(destinationTable));
    }

    // After, regardless of whether we flushed or not, clean up old batches from the intermediate
    // table. Some rows may be several batches old but still in the table if they were in the
    // streaming buffer during the last purge.
    logger.trace("Clearing batches from {} on back from {}", batchNumber, intTable(intermediateTable));
    String batchClearQuery = batchClearQuery(intermediateTable, batchNumber);
    logger.trace(batchClearQuery);
    bigQuery.query(QueryJobConfiguration.of(batchClearQuery));
  }

  /*

    upsert+delete:

    MERGE `<dataset>`.`<destinationTable>`
    USING (
      SELECT * FROM (
        SELECT ARRAY_AGG(
          x ORDER BY i DESC LIMIT 1
        )[OFFSET(0)] src
        FROM `<dataset>`.`<intermediateTable>` x
        WHERE batchNumber=<batchNumber>
        GROUP BY key.<field>[, key.<field>...]
      )
    )
    ON `<destinationTable>`.<keyField>=`src`.key
    WHEN MATCHED AND `src`.value IS NOT NULL
      THEN UPDATE SET <valueField>=`src`.value.<field>[, <valueField>=`src`.value.<field>...]
    WHEN MATCHED AND `src`.value IS NULL
      THEN DELETE
    WHEN NOT MATCHED AND `src`.value IS NOT NULL
      THEN INSERT (<keyField>, _PARTITIONTIME, <valueField>[, <valueField>])
      VALUES (
        `src`.key,
        CAST(CAST(DATE(`src`.partitionTime) AS DATE) AS TIMESTAMP),
        `src`.value.<field>[, `src`.value.<field>...]
      );


    delete only:
    
    MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT batch.key AS key, value, partitionTime
         FROM (
          SELECT src.i, src.key FROM (
            SELECT ARRAY_AGG(
              x ORDER BY i DESC LIMIT 1
            )[OFFSET(0)] src
            FROM (
              SELECT * FROM `<dataset>`.`<intermediateTable>`
              WHERE batchNumber=<batchNumber>
            ) x
            WHERE x.value IS NULL
            GROUP BY key.<field>[, key.<field>...])) AS deletes
          RIGHT JOIN (
            SELECT * FROM `<dataset>`.`<intermediateTable`
            WHERE batchNumber=<batchNumber>
          ) AS batch
          USING (key)
        WHERE deletes.i IS NULL OR batch.i >= deletes.i
        ORDER BY batch.i ASC) AS src
      ON `<destinationTable>`.<keyField>=src.key AND src.value IS NULL
      WHEN MATCHED
        THEN DELETE
      WHEN NOT MATCHED AND src.value IS NOT NULL
      THEN INSERT (<keyField>, _PARTITIONTIME, <valueField>[, <valueField>])
      VALUES (
        `src`.key,
        CAST(CAST(DATE(`src`.partitionTime) AS DATE) AS TIMESTAMP),
        `src`.value.<field>[, `src`.value.<field>...]
      );


    upsert only:

    MERGE `<dataset>`.`<destinationTable>`
    USING (
      SELECT * FROM (
        SELECT ARRAY_AGG(
          x ORDER BY i DESC LIMIT 1
        )[OFFSET(0)] src
        FROM `<dataset>`.`<intermediateTable>` x
        WHERE batchNumber=<batchNumber>
        GROUP BY key.<field>[, key.<field>...]
      )
    )
    ON `<destinationTable>`.<keyField>=`src`.key
    WHEN MATCHED
      THEN UPDATE SET <valueField>=`src`.value.<field>[, <valueField=`src.value.<field>...]
    WHEN NOT MATCHED
      THEN INSERT (<keyField, _PARTITIONTIME, <valueField[, <valueField])
      VALUES (
        `src`.key,
        CAST(CAST(DATE(`src`.partitionTime) AS DATE) AS TIMESTAMP),
        `src`.value.<field>[, `src`.value.<field>...]
      );

   */
  @VisibleForTesting
  String mergeFlushQuery(TableId intermediateTable, TableId destinationTable, int batchNumber) {
    Schema intermediateSchema = schemaManager.cachedSchema(intermediateTable);

    String srcKey = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;

    List<String> keyFields = listFields(
        intermediateSchema.getFields().get(srcKey).getSubFields(),
        srcKey + "."
    );
    List<String> dstValueFields = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields()
        .stream()
        .map(Field::getName)
        .collect(Collectors.toList());

    List<String> srcValueFields = dstValueFields.stream()
        .map(field -> "src." + INTERMEDIATE_TABLE_VALUE_FIELD_NAME + "." + field)
        .collect(Collectors.toList());
    List<String> updateValues = dstValueFields.stream()
        .map(field -> field + "=src." + INTERMEDIATE_TABLE_VALUE_FIELD_NAME + "." + field)
        .collect(Collectors.toList());

    String partitionTimeSrcColumn = insertPartitionTime ? INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ", " : "";
    String partitionTimePseudocolumn = insertPartitionTime ? "_PARTITIONTIME, " : "";
    String partitionTimeValue = insertPartitionTime
        ? "CAST(CAST(DATE(src." + INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ") AS DATE) AS TIMESTAMP), "
        : "";

    String dst = destinationTable.getTable();

    StringBuilder intTable = new StringBuilder("`").append(intermediateTable.getDataset())
        .append("`.`").append(intermediateTable.getTable())
        .append("` ");

    StringBuilder keysMatch = new StringBuilder("`").append(dst).append("`.").append(keyFieldName).append("=src.").append(srcKey).append(" ");
    StringBuilder batchNumberClause = new StringBuilder("WHERE ").append(INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD).append("=").append(batchNumber);
    StringBuilder deduplicatedAsX = new StringBuilder("SELECT ARRAY_AGG(")
        .append("x ORDER BY ").append(i).append(" DESC LIMIT 1")
        .append(")[OFFSET(0)] src ");
    StringBuilder groupByKeys = new StringBuilder("GROUP BY ").append(String.join(", ", keyFields));

    StringBuilder mergeOpening = new StringBuilder("MERGE `").append(destinationTable.getDataset()).append("`.`")
        .append(destinationTable.getTable()).append("` ")
        .append("USING ");

    /*
      Delete-only is the trickiest mode. Naively, we could just run a MERGE using the intermediate table as a source
      and sort in ascending order of iteration. However, this would miss an edge case where, for a given key,
      a non-tombstone record is sent, followed by a tombstone, and would result in all rows with that key being deleted
      from the table, followed by an insertion of a row for the initial non-tombstone record. This is incorrect; any and
      all records with a given key that precede a tombstone should either never make it into BigQuery or be deleted once
      the tombstone record is merge flushed.

      So instead, we have to try to filter out rows from the source (i.e., intermediate) table that precede tombstone
      records for their keys. We do this by:

        - Finding the latest tombstone row for each key in the current batch and extracting the iteration number for each, referring to this as the "deletes" table
        - Joining that with the current batch from the intermediate table on the row key, keeping both tables' iteration numbers
          (a RIGHT JOIN is used so that rows whose keys don't have any tombstones present are included with a NULL iteration number for the "deletes" table)
        - Filtering out all rows where the "delete" table's iteration number is non-null, and their iteration number is less than the "delete" table's iteration number

      This gives us only rows from the most recent tombstone onward, and works in both cases where the most recent row for a key is or is not a tombstone.

     */
    StringBuilder deleteSrc = new StringBuilder("(SELECT ")
        .append("batch.").append(srcKey).append(" AS ").append(srcKey).append(", ")
        .append(partitionTimeSrcColumn)
        .append(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).append(" ")
        .append("FROM (")
          .append("SELECT src.").append(i).append(", ")
          .append("src.").append(srcKey).append(" ")
          .append("FROM (").append(deduplicatedAsX)
            .append("FROM (")
              .append("SELECT * FROM ").append(intTable)
              .append(batchNumberClause)
            .append(") x ")
            .append("WHERE x.value IS NULL ")
            .append(groupByKeys).append(")) AS deletes ")
          .append("RIGHT JOIN (")
            .append("SELECT * FROM ").append(intTable)
            .append(batchNumberClause)
          .append(") AS batch ")
          .append("USING (").append(srcKey).append(") ")
        .append("WHERE deletes.").append(i).append(" IS NULL OR batch.").append(i).append(" >= deletes.").append(i).append(" ")
        .append("ORDER BY batch.").append(i).append(" ASC) AS src ");

    StringBuilder deduplicatedSrc = new StringBuilder("(SELECT * FROM (")
        .append(deduplicatedAsX)
          .append("FROM ").append(intTable).append("x ")
          .append(batchNumberClause).append(" ")
          .append("GROUP BY ").append(String.join(", ", keyFields))
        .append(")) ");

    StringBuilder insertClause = new StringBuilder("THEN INSERT (")
          .append(keyFieldName).append(", ")
          .append(partitionTimePseudocolumn)
          .append(String.join(", ", dstValueFields))
        .append(") ")
        .append("VALUES (")
          .append("src.").append(srcKey).append(", ")
          .append(partitionTimeValue)
          .append(String.join(", ", srcValueFields))
        .append(")");

    StringBuilder updateClause = new StringBuilder("THEN UPDATE SET ")
        .append(String.join(", ", updateValues));

    StringBuilder valueIs = new StringBuilder("src.").append(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).append(" IS ");

    if (upsertEnabled && deleteEnabled) {
      // Delete rows with null values, and upsert all others
      return mergeOpening
          .append(deduplicatedSrc)
          .append("ON ").append(keysMatch)
          .append("WHEN MATCHED AND ").append(valueIs).append("NOT NULL ")
            .append(updateClause).append(" ")
          .append("WHEN MATCHED AND ").append(valueIs).append("NULL ")
            .append("THEN DELETE ")
          .append("WHEN NOT MATCHED AND ").append(valueIs).append("NOT NULL ")
            .append(insertClause)
          .append(";")
          .toString();
    } else if (upsertEnabled) {
      // Assume all rows have non-null values and upsert them all
      return mergeOpening
          .append(deduplicatedSrc)
          .append("ON ").append(keysMatch)
          .append("WHEN MATCHED ")
            .append(updateClause).append(" ")
          .append("WHEN NOT MATCHED ")
            .append(insertClause)
          .append(";")
          .toString();
    } else if (deleteEnabled) {
      // Delete rows with null values, and insert all others
      return mergeOpening
          .append(deleteSrc)
          .append("ON ").append(keysMatch)
            .append("AND ").append(valueIs).append("NULL ")
          .append("WHEN MATCHED ")
            .append("THEN DELETE ")
          .append("WHEN NOT MATCHED AND ").append(valueIs).append("NOT NULL ")
            .append(insertClause)
          .append(";")
          .toString();
    } else {
      throw new IllegalStateException("At least one of upsert or delete must be enabled for merge flushing to occur.");
    }
  }

  // DELETE FROM `<dataset>`.`<intermediateTable>` WHERE batchNumber <= <batchNumber> AND _PARTITIONTIME IS NOT NULL;
  @VisibleForTesting
  static String batchClearQuery(TableId intermediateTable, int batchNumber) {
    return new StringBuilder("DELETE FROM `").append(intermediateTable.getDataset()).append("`.`").append(intermediateTable.getTable()).append("` ")
        .append("WHERE ")
          .append(INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD).append(" <= ").append(batchNumber).append(" ")
          // Use this clause to filter out rows that are still in the streaming buffer, which should
          // not be subjected to UPDATE or DELETE operations or the query will FAIL
          .append("AND _PARTITIONTIME IS NOT NULL")
        .append(";")
        .toString();
  }

  private static List<String> listFields(FieldList keyFields, String prefix) {
    return keyFields.stream()
        .flatMap(field -> {
          String fieldName = prefix + field.getName();
          FieldList subFields = field.getSubFields();
          if (subFields == null) {
            return Stream.of(fieldName);
          }
          return listFields(subFields, fieldName + ".").stream();
        }).collect(Collectors.toList());
  }
}
