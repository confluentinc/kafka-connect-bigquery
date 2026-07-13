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

package com.wepay.kafka.connect.bigquery.exception;

import com.google.cloud.bigquery.BigQueryError;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BigQueryConnectExceptionTest {

  @Test
  public void shouldNotIncludeBigQueryErrorMessageInInsertAllErrors() {
    // BigQuery's per-row message embeds the raw rejected record value; it must not leak into the
    // exception message (which is logged at ERROR by the framework LogReporter). See CC-42851.
    String sensitiveRecordValue = "1780000000000000000";
    BigQueryError error = new BigQueryError(
        "invalid",
        "timestamp_field",
        "Timestamp field value is out of range: " + sensitiveRecordValue);
    Map<Long, List<BigQueryError>> errors =
        Collections.singletonMap(0L, Collections.singletonList(error));

    BigQueryConnectException exception = new BigQueryConnectException("test_table", errors);
    String message = exception.getMessage();

    // Safe diagnostic fields are retained
    Assert.assertTrue(message.contains("test_table"));
    Assert.assertTrue(message.contains("row index 0"));
    Assert.assertTrue(message.contains("timestamp_field"));
    Assert.assertTrue(message.contains("invalid"));

    // The free-form BigQuery message (and the record value it carries) is dropped
    Assert.assertFalse(message.contains(sensitiveRecordValue));
    Assert.assertFalse(message.contains("Timestamp field value is out of range"));
  }
}
