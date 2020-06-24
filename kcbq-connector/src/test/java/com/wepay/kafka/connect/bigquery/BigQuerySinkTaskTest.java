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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class BigQuerySinkTaskTest {
  private static SinkTaskPropertiesFactory propertiesFactory;

  @BeforeClass
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  @Before
  public void setUp() {
    MergeBatches.setStreamingBufferAvailabilityWait(0);
  }

  @After
  public void cleanUp() {
    MergeBatches.resetStreamingBufferAvailabilityWait();
  }

  @Test
  public void testSimplePut() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testSimplePutWhenSchemaRetrieverIsNotNull() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
    verify(schemaRetriever, times(1)).setLastSeenSchema(any(TableId.class),
        any(String.class), any(Schema.class));
  }

  @Test
  public void testEmptyPut() {
    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.start(properties);

    testTask.put(Collections.emptyList());
  }

  // needed because debezium sends null messages when deleting messages in kafka
  @Test
  public void testEmptyRecordPut() {
    final String topic = "test_topic";
    final Schema simpleSchema = SchemaBuilder
        .struct()
        .field("aField", Schema.STRING_SCHEMA)
        .build();

    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.start(properties);

    SinkRecord emptyRecord = spoofSinkRecord(topic, simpleSchema, null);

    testTask.put(Collections.singletonList(emptyRecord));
  }

  @Test
  public void testPutWhenPartitioningOnMessageTime() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);

    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic$20171026", argument.getValue().getTable().getTable());
  }
  
  @Test
  public void testPutWhenPartitioningIsSetToTrue() {
    final String topic = "test-topic";
    
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");
    
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    
    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    
    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);
    
    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic$20171026", argument.getValue().getTable().getTable());
  }
  
  @Test
  public void testPutWhenPartitioningIsSetToFalse() {
    final String topic = "test-topic";
    
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    
    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    
    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);
    
    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic", argument.getValue().getTable().getTable());
  }

  // Make sure a connect exception is thrown when the message has no timestamp type
  @Test(expected = ConnectException.class)
  public void testPutWhenPartitioningOnMessageTimeWhenNoTimestampType() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.NO_TIMESTAMP_TYPE, null)));
  }

  @Test
  public void testPutWithUpsertDelete() throws Exception {
    final String topic = "test-topic";
    final String key = "kafkaKey";
    final String value = "recordValue";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    properties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    properties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    properties.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "-1");
    properties.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, "2");
    properties.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, key);

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Field keyField = Field.of(key, LegacySQLTypeName.STRING);
    Field valueField = Field.of(value, LegacySQLTypeName.STRING);
    com.google.cloud.bigquery.Schema intermediateSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, LegacySQLTypeName.RECORD, keyField)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, LegacySQLTypeName.RECORD, valueField)
            .build()
    );
    when(schemaManager.cachedSchema(any())).thenReturn(intermediateSchema);

    CountDownLatch executedMerges = new CountDownLatch(2);
    CountDownLatch executedBatchClears = new CountDownLatch(2);

    when(bigQuery.query(any(QueryJobConfiguration.class))).then(invocation -> {
      String query = invocation.getArgument(0, QueryJobConfiguration.class).getQuery();
      if (query.startsWith("MERGE")) {
        executedMerges.countDown();
      } else if (query.startsWith("DELETE")) {
        executedBatchClears.countDown();
      }
      return null;
    });

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    // Insert a few regular records and one tombstone record
    testTask.put(Arrays.asList(
        spoofSinkRecord(topic, key, "4761", "value", "message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "489", "value", "other message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "28980", "value", "more message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "4761", null, null, TimestampType.NO_TIMESTAMP_TYPE, null)
    ));

    assertTrue("Merge queries should be executed", executedMerges.await(5, TimeUnit.SECONDS));
    assertTrue("Batch clears should be executed", executedBatchClears.await(1, TimeUnit.SECONDS));
  }

  // It's important that the buffer be completely wiped after a call to flush, since any exception
  // thrown during flush causes Kafka Connect to not commit the offsets for any records sent to the
  // task since the last flush
  @Test
  public void testBufferClearOnFlushError() {
    final String dataset = "scratch";
    final String topic = "test_topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(new RuntimeException("This is a test"));

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    try {
      testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
      testTask.flush(Collections.emptyMap());
      fail("An exception should have been thrown by now");
    } catch (BigQueryConnectException err) {
      testTask.flush(Collections.emptyMap());
      verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
    }
  }

  @Test
  public void testEmptyFlush() {
    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.flush(Collections.emptyMap());
  }

  @Test
  public void testBigQuery5XXRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "3");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(500, "mock 500"))
        .thenThrow(new BigQueryException(502, "mock 502"))
        .thenThrow(new BigQueryException(503, "mock 503"))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(4)).insertAll(anyObject());
  }

  @Test
  public void testBigQuery403Retry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "2");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    BigQueryError rateLimitExceededError = new BigQueryError("rateLimitExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError))
        .thenThrow(new BigQueryException(403, "mock rate limit exceeded", rateLimitExceededError))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(3)).insertAll(anyObject());
  }

  @Test(expected = BigQueryConnectException.class)
  public void testBigQueryRetryExceeded() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "1");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
      .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError));
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());
  }

  // Make sure that an InterruptedException is properly translated into a ConnectException
  @Test(expected = ConnectException.class)
  public void testInterruptedException() {
    final String dataset = "scratch";
    final String topic = "test_topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery  = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    InsertAllResponse fakeResponse = mock(InsertAllResponse.class);
    when(fakeResponse.hasErrors()).thenReturn(false);
    when(fakeResponse.getInsertErrors()).thenReturn(Collections.emptyMap());
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(fakeResponse);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    Thread.currentThread().interrupt();
    testTask.flush(Collections.emptyMap());
  }

  // Make sure that a ConfigException is properly translated into a SinkConfigConnectException
  @Test(expected = SinkConfigConnectException.class)
  public void testConfigException() {
    try {
      Map<String, String> badProperties = propertiesFactory.getProperties();
      badProperties.remove(BigQuerySinkConfig.TOPICS_CONFIG);
      BigQuerySinkConfig.validate(badProperties);
  
      SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
      SchemaManager schemaManager = mock(SchemaManager.class);
  
      BigQuerySinkTask testTask =
          new BigQuerySinkTask(mock(BigQuery.class), schemaRetriever, mock(Storage.class), schemaManager);
      testTask.start(badProperties);
    } catch (ConfigException e) {
      throw new SinkConfigConnectException(e);
    }
  }

  @Test
  public void testVersion() {
    assertNotNull(new BigQuerySinkTask().version());
  }

  // Existing tasks should succeed upon stop is called. New tasks should be rejected once task is
  // stopped.
  @Test(expected = RejectedExecutionException.class)
  public void testStop() {
    final String dataset = "scratch";
    final String topic = "test_topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    Storage storage = mock(Storage.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));

    assertEquals(1, testTask.getTaskThreadsActiveCount());
    testTask.stop();
    assertEquals(0, testTask.getTaskThreadsActiveCount());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * @param topic The topic of the record.
   * @param keyField The field name for the record key; may be null.
   * @param key The content of the record key; may be null.
   * @param valueField The field name for the record value; may be null
   * @param value The content of the record value; may be null
   * @param timestampType The type of timestamp embedded in the message
   * @param timestamp The timestamp in milliseconds
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String keyField, String key,
                                           String valueField, String value,
                                           TimestampType timestampType, Long timestamp) {
    Schema basicKeySchema = null;
    Struct basicKey = null;
    if (keyField != null) {
      basicKeySchema = SchemaBuilder
          .struct()
          .field(keyField, Schema.STRING_SCHEMA)
          .build();
      basicKey = new Struct(basicKeySchema);
      basicKey.put(keyField, key);
    }

    Schema basicValueSchema = null;
    Struct basicValue = null;
    if (valueField != null) {
      basicValueSchema = SchemaBuilder
          .struct()
          .field(valueField, Schema.STRING_SCHEMA)
          .build();
      basicValue = new Struct(basicValueSchema);
      basicValue.put(valueField, value);
    }

    return new SinkRecord(topic, 0, basicKeySchema, basicKey,
        basicValueSchema, basicValue, 0, timestamp, timestampType);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * @param topic The topic of the record.
   * @param field The field name for the record value.
   * @param value The content of the record value.
   * @param timestampType The type of timestamp embedded in the message
   * @param timestamp The timestamp in milliseconds
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String field, String value,
                                           TimestampType timestampType, Long timestamp) {
    return spoofSinkRecord(topic, null, null, field, value, timestampType, timestamp);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * @param topic The topic of the record.
   * @param valueSchema The schema of the record.
   * @param value The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, Schema valueSchema, Struct value) {
    return new SinkRecord(topic, 0, null, null, valueSchema, value, 0);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a given name and value.
   * @param topic The topic of the record.
   * @param field The name of the field in the record's struct.
   * @param value The content of the field.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String field, String value) {
    return spoofSinkRecord(topic, field, value, TimestampType.NO_TIMESTAMP_TYPE, null);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a given
   * value.
   * @param topic The topic of the record.
   * @param value The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String value) {
    return spoofSinkRecord(topic, "sink_task_test_field", value);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a
   * default value.
   * @param topic The topic of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic) {
    return spoofSinkRecord(topic, "sink task test row");
  }
}
