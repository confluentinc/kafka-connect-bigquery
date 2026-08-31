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

package com.wepay.kafka.connect.bigquery.write.batch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class TableWriterTest {

  // A synthetic value standing in for a rejected record field value that BigQuery can echo
  // back in the BigQueryError message on an invalid/conversion error. It must never appear
  // in the WARN log emitted when a batch write fails.
  private static final String SENSITIVE_CANARY = "super-secret-record-value-42";

  private Logger tableWriterLogger;
  private Level originalLevel;
  private CapturingAppender appender;

  @Before
  public void setUp() {
    tableWriterLogger = Logger.getLogger(TableWriter.class);
    // src/test/resources/log4j.properties pins this logger to ERROR to keep routine test
    // output quiet; lower it so the failure-path WARN is actually emitted and captured.
    originalLevel = tableWriterLogger.getLevel();
    tableWriterLogger.setLevel(Level.WARN);
    appender = new CapturingAppender();
    tableWriterLogger.addAppender(appender);
  }

  @After
  public void tearDown() {
    tableWriterLogger.removeAppender(appender);
    tableWriterLogger.setLevel(originalLevel);
  }

  @Test
  public void testBatchWriteFailureDoesNotLogRecordValue() throws InterruptedException {
    BigQueryWriter writer = mock(BigQueryWriter.class);
    PartitionedTableId table = new PartitionedTableId.Builder("dataset", "table").build();

    // A request-level BigQueryException whose BigQueryError.message carries the rejected value.
    // The exception message is set to the canary too, so the red run also proves the full
    // exception is no longer logged as the WARN's throwable.
    BigQueryError error = new BigQueryError("backendError", "field_location", SENSITIVE_CANARY);
    BigQueryException bigQueryException = new BigQueryException(400, SENSITIVE_CANARY, error);
    doThrow(bigQueryException).when(writer).writeRows(any(), any());

    SortedMap<SinkRecord, RowToInsert> rows = new TreeMap<>(
        Comparator.comparing(SinkRecord::kafkaPartition).thenComparing(SinkRecord::kafkaOffset));
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
    rows.put(record, RowToInsert.of(Collections.singletonMap("f1", SENSITIVE_CANARY)));

    TableWriter tableWriter = new TableWriter(writer, table, rows, r -> { });

    // "backendError" is not a batch-size error, so after the WARN the writer rethrows to the
    // framework; asserting the throw proves the WARN branch actually ran.
    assertThrows(BigQueryConnectException.class, tableWriter::run);

    List<LoggingEvent> warnEvents = appender.eventsAtLeast(Level.WARN);
    assertFalse("Expected a WARN log from the batch-write failure", warnEvents.isEmpty());

    for (LoggingEvent event : warnEvents) {
      String rendered = String.valueOf(event.getRenderedMessage());
      String throwable = "";
      if (event.getThrowableStrRep() != null) {
        throwable = String.join("\n", event.getThrowableStrRep());
      }
      assertFalse(
          "WARN log must not contain the rejected record value: " + rendered,
          rendered.contains(SENSITIVE_CANARY) || throwable.contains(SENSITIVE_CANARY));
      assertTrue(
          "WARN log should retain the safe reason correlator",
          rendered.contains("backendError"));
      assertTrue(
          "WARN log should retain the safe field-location correlator",
          rendered.contains("field_location"));
    }
  }

  private static final class CapturingAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }

    List<LoggingEvent> eventsAtLeast(Level level) {
      List<LoggingEvent> result = new ArrayList<>();
      for (LoggingEvent event : events) {
        if (event.getLevel().isGreaterOrEqual(level)) {
          result.add(event);
        }
      }
      return result;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }
}
