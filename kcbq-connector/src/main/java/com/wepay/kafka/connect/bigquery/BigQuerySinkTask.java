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

package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import com.wepay.kafka.connect.bigquery.utils.Version;
import com.wepay.kafka.connect.bigquery.write.batch.GCSBatchTableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  private SchemaRetriever schemaRetriever;
  private BigQueryWriter bigQueryWriter;
  private GCSToBQWriter gcsToBQWriter;
  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;
  private Map<String, TableId> topicsToBaseTableIds;
  private boolean useMessageTimeDatePartitioning;
  private boolean usePartitionDecorator;

  private TopicPartitionManager topicPartitionManager;

  private KCBQThreadPoolExecutor executor;
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 30;
  
  private final BigQuery testBigQuery;
  private final Storage testGcs;
  private final SchemaManager testSchemaManager;

  private final UUID uuid = UUID.randomUUID();
  private ScheduledExecutorService gcsLoadExecutor;

  /**
   * Create a new BigquerySinkTask.
   */
  public BigQuerySinkTask() {
    testBigQuery = null;
    schemaRetriever = null;
    testGcs = null;
    testSchemaManager = null;
  }

  /**
   * For testing purposes only; will never be called by the Kafka Connect framework.
   *
   * @param testBigQuery {@link BigQuery} to use for testing (likely a mock)
   * @param schemaRetriever {@link SchemaRetriever} to use for testing (likely a mock)
   * @param testGcs {@link Storage} to use for testing (likely a mock)
   * @param testSchemaManager {@link SchemaManager} to use for testing (likely a mock)
   * @see BigQuerySinkTask#BigQuerySinkTask()
   */
  public BigQuerySinkTask(BigQuery testBigQuery, SchemaRetriever schemaRetriever, Storage testGcs, SchemaManager testSchemaManager) {
    this.testBigQuery = testBigQuery;
    this.schemaRetriever = schemaRetriever;
    this.testGcs = testGcs;
    this.testSchemaManager = testSchemaManager;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    try {
      executor.awaitCurrentTasks();
    } catch (InterruptedException err) {
      throw new ConnectException("Interrupted while waiting for write tasks to complete.", err);
    }

    topicPartitionManager.resumeAll();
  }

  private PartitionedTableId getRecordTable(SinkRecord record) {
    // Dynamically update topicToBaseTableIds mapping. topicToBaseTableIds was used to be
    // constructed when connector starts hence new topic configuration needed connector to restart.
    // Dynamic update shall not require connector restart and shall compute table id in runtime.
    if (!topicsToBaseTableIds.containsKey(record.topic())) {
      TopicToTableResolver.updateTopicToTable(config, record.topic(), topicsToBaseTableIds);
    }

    TableId baseTableId = topicsToBaseTableIds.get(record.topic());

    PartitionedTableId.Builder builder = new PartitionedTableId.Builder(baseTableId);
    if (usePartitionDecorator) {

      if (useMessageTimeDatePartitioning) {
        if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
          throw new ConnectException(
              "Message has no timestamp type, cannot use message timestamp to partition.");
        }
        builder.setDayPartition(record.timestamp());
      } else {
        builder.setDayPartitionForNow();
      }
    }

    return builder.build();
  }

  private RowToInsert getRecordRow(SinkRecord record) {
    Map<String, Object> convertedRecord = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);
    Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
    if (kafkaKeyFieldName.isPresent()) {
      convertedRecord.put(kafkaKeyFieldName.get(), recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY));
    }
    Optional<String> kafkaDataFieldName = config.getKafkaDataFieldName();
    if (kafkaDataFieldName.isPresent()) {
      convertedRecord.put(kafkaDataFieldName.get(), KafkaDataBuilder.buildKafkaDataRecord(record));
    }
    if (config.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)) {
      convertedRecord = FieldNameSanitizer.replaceInvalidKeys(convertedRecord);
    }
    return RowToInsert.of(getRowId(record), convertedRecord);
  }

  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    // Periodically poll for errors here instead of doing a stop-the-world check in flush()
    executor.maybeThrowEncounteredError();

    logger.debug("Putting {} records in the sink.", records.size());

    // create tableWriters
    Map<PartitionedTableId, TableWriterBuilder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null) {
        PartitionedTableId table = getRecordTable(record);
        if (schemaRetriever != null) {
          schemaRetriever.setLastSeenSchema(table.getBaseTableId(),
                                            record.topic(),
                                            record.valueSchema());
        }

        if (!tableWriterBuilders.containsKey(table)) {
          TableWriterBuilder tableWriterBuilder;
          if (config.getList(BigQuerySinkConfig.ENABLE_BATCH_CONFIG).contains(record.topic())) {
            String topic = record.topic();
            String gcsBlobName = topic + "_" + uuid + "_" + Instant.now().toEpochMilli();
            String gcsFolderName = config.getString(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG);
            if (gcsFolderName != null && !"".equals(gcsFolderName)) {
              gcsBlobName = gcsFolderName + "/" + gcsBlobName;
            }
            tableWriterBuilder = new GCSBatchTableWriter.Builder(
                gcsToBQWriter,
                table.getBaseTableId(),
                config.getString(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG),
                gcsBlobName,
                topic,
                recordConverter);
          } else {
            tableWriterBuilder =
                new TableWriter.Builder(bigQueryWriter, table, record.topic(), recordConverter);
          }
          tableWriterBuilders.put(table, tableWriterBuilder);
        }
        tableWriterBuilders.get(table).addRow(getRecordRow(record));
      }
    }

    // add tableWriters to the executor work queue
    for (TableWriterBuilder builder : tableWriterBuilders.values()) {
      executor.execute(builder.build());
    }

    // check if we should pause topics
    long queueSoftLimit = config.getLong(BigQuerySinkTaskConfig.QUEUE_SIZE_CONFIG);
    if (queueSoftLimit != -1) {
      int currentQueueSize = executor.getQueue().size();
      if (currentQueueSize > queueSoftLimit) {
        topicPartitionManager.pauseAll();
      } else if (currentQueueSize <= queueSoftLimit / 2) {
        // resume only if there is a reasonable chance we won't immediately have to pause again.
        topicPartitionManager.resumeAll();
      }
    }
  }

  private RecordConverter<Map<String, Object>> getConverter() {
    return config.getRecordConverter();
  }

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    return new GcpClientBuilder.BigQueryBuilder()
        .withConfig(config)
        .build();
  }

  private SchemaManager getSchemaManager(BigQuery bigQuery) {
    if (testSchemaManager != null) {
      return testSchemaManager;
    }
    schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter =
        config.getSchemaConverter();
    Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
    Optional<String> kafkaDataFieldName = config.getKafkaDataFieldName();
    Optional<String> timestampPartitionFieldName = config.getTimestampPartitionFieldName();
    Optional<List<String>> clusteringFieldName = config.getClusteringPartitionFieldNames();
    return new SchemaManager(schemaRetriever, schemaConverter, bigQuery, kafkaKeyFieldName,
                             kafkaDataFieldName, timestampPartitionFieldName, clusteringFieldName);
  }

  private BigQueryWriter getBigQueryWriter() {
    boolean autoUpdateSchemas = config.getBoolean(BigQuerySinkConfig.SCHEMA_UPDATE_CONFIG);
    boolean autoCreateTables = config.getBoolean(BigQuerySinkConfig.TABLE_CREATE_CONFIG);
    int retry = config.getInt(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG);
    BigQuery bigQuery = getBigQuery();
    if (autoUpdateSchemas || autoCreateTables) {
      return new AdaptiveBigQueryWriter(bigQuery,
                                        getSchemaManager(bigQuery),
                                        retry,
                                        retryWait,
                                        autoUpdateSchemas,
                                        autoCreateTables);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait);
    }
  }

  private Storage getGcs() {
    if (testGcs != null) {
      return testGcs;
    }
    return new GcpClientBuilder.GcsBuilder()
        .withConfig(config)
        .build();
  }

  private GCSToBQWriter getGcsWriter() {
    BigQuery bigQuery = getBigQuery();
    int retry = config.getInt(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG);
    boolean autoCreateTables = config.getBoolean(BigQuerySinkConfig.TABLE_CREATE_CONFIG);
    // schemaManager shall only be needed for creating table hence do not fetch instance if not
    // needed.
    SchemaManager schemaManager = autoCreateTables ? getSchemaManager(bigQuery) : null;
    return new GCSToBQWriter(getGcs(),
                         bigQuery,
                         schemaManager,
                         retry,
                         retryWait,
                         autoCreateTables);
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    config = new BigQuerySinkTaskConfig(properties);

    bigQueryWriter = getBigQueryWriter();
    gcsToBQWriter = getGcsWriter();
    topicsToBaseTableIds = TopicToTableResolver.getTopicsToTables(config);
    recordConverter = getConverter();
    executor = new KCBQThreadPoolExecutor(config, new LinkedBlockingQueue<>());
    topicPartitionManager = new TopicPartitionManager();
    useMessageTimeDatePartitioning =
        config.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
    usePartitionDecorator = 
            config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG);
    if (config.getBoolean(BigQuerySinkTaskConfig.GCS_BQ_TASK_CONFIG)) {
      startGCSToBQLoadTask();
    }
  }

  private void startGCSToBQLoadTask() {
    logger.info("Attempting to start GCS Load Executor.");
    gcsLoadExecutor = Executors.newScheduledThreadPool(1);
    String bucketName = config.getString(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG);
    Storage gcs = getGcs();
    // get the bucket, or create it if it does not exist.
    Bucket bucket = gcs.get(bucketName);
    if (bucket == null) {
      // todo here is where we /could/ set a retention policy for the bucket,
      // but for now I don't think we want to do that.
      BucketInfo bucketInfo = BucketInfo.of(bucketName);
      bucket = gcs.create(bucketInfo);
    }
    GCSToBQLoadRunnable loadRunnable = new GCSToBQLoadRunnable(getBigQuery(), bucket);

    int intervalSec = config.getInt(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG);
    gcsLoadExecutor.scheduleAtFixedRate(loadRunnable, intervalSec, intervalSec, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    try {
      executor.shutdown();
      executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
      if (gcsLoadExecutor != null) {
        try {
          logger.info("Attempting to shut down GCS Load Executor.");
          gcsLoadExecutor.shutdown();
          gcsLoadExecutor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
          logger.warn("Could not shut down GCS Load Executor within {}s.",
                      EXECUTOR_SHUTDOWN_TIMEOUT_SEC);
        }
      }
    } catch (InterruptedException ex) {
      logger.warn("{} active threads are still executing tasks {}s after shutdown is signaled.",
          executor.getActiveCount(), EXECUTOR_SHUTDOWN_TIMEOUT_SEC);
    } finally {
      logger.trace("task.stop()");
    }
  }

  @VisibleForTesting
  int getTaskThreadsActiveCount() {
    return executor.getActiveCount();
  }

  @Override
  public String version() {
    String version = Version.version();
    logger.trace("task.version() = {}", version);
    return version;
  }

  private class TopicPartitionManager {

    private Long lastChangeMs;
    private boolean isPaused;

    public TopicPartitionManager() {
      this.lastChangeMs = System.currentTimeMillis();
      this.isPaused = false;
    }

    public void pauseAll() {
      if (!isPaused) {
        long now = System.currentTimeMillis();
        logger.warn("Paused all partitions after {}ms", now - lastChangeMs);
        isPaused = true;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.pause(assignment.toArray(new TopicPartition[assignment.size()]));
    }

    public void resumeAll() {
      if (isPaused) {
        long now = System.currentTimeMillis();
        logger.info("Resumed all partitions after {}ms", now - lastChangeMs);
        isPaused = false;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.resume(assignment.toArray(new TopicPartition[assignment.size()]));
    }
  }
}
