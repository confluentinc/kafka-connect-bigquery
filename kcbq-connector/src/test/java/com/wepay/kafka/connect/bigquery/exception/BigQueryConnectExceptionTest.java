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
  public void shouldNotLeakRecordValueFromBigQueryErrorMessage() {
    // The BigQueryError message can embed the raw rejected record value (CC-42851)
    BigQueryError error = new BigQueryError(
        "invalid", "timestamp_field", "value out of range: 1780000000000000000");
    Map<Long, List<BigQueryError>> errors =
        Collections.singletonMap(0L, Collections.singletonList(error));

    String message = new BigQueryConnectException("test_table", errors).getMessage();

    Assert.assertFalse(message.contains("1780000000000000000"));
  }
}
