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

package com.wepay.kafka.connect.bigquery.convert.logicaltype;

import static com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.EPOCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.bigquery.LegacySQLTypeName;

import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.ZonedTimestampConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DebeziumLogicalConvertersTest {

  private static final Integer MILLI_TIMESTAMP_INT = 1488406838;
  private static final Long MILLI_TIMESTAMP = 1488406838808L;
  private static final Long MICRO_TIMESTAMP = 1488406838808123L;

  private final String underTestDate;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList(
            "0100-02-28",
            "0100-03-01",
            "0200-02-28",
            "0200-03-01",
            "0300-02-28",
            "0300-03-01",
            "0400-02-28",
            "0400-02-29",
            "0400-03-01",
            "0500-02-28",
            "0500-03-01",
            "0600-02-28",
            "0600-03-01",
            "0700-02-28",
            "0700-03-01",
            "0800-02-28",
            "0800-02-29",
            "0800-03-01",
            "0900-02-28",
            "0900-03-01",
            "1000-02-28",
            "1000-03-01",
            "1100-02-28",
            "1100-03-01",
            "1200-02-28",
            "1200-02-29",
            "1200-03-01",
            "1300-02-28",
            "1300-03-01",
            "1400-02-28",
            "1400-03-01",
            "1500-02-28",
            "1500-03-01",
            "1582-10-01",
            "1582-10-02",
            "1582-10-03",
            "1582-10-04",
            "1582-10-15",
            "1582-10-16",
            "1582-10-17",
            "1582-10-18",
            "1582-10-19",
            "1582-10-20",
            "1582-10-21",
            "1582-10-22",
            "1582-10-23",
            "1582-10-24",
            "1582-10-25",
            "1582-10-26",
            "1582-10-27",
            "1582-10-28",
            "1582-10-29",
            "1582-10-30",
            "1582-10-31",

            "2017-03-01"
    );
  }

  public DebeziumLogicalConvertersTest(String underTestDate) {
    this.underTestDate = underTestDate;
  }

  @Test
  public void testDateConversion() {
    DateConverter converter = new DateConverter();

    assertEquals(LegacySQLTypeName.DATE, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    int epochDays = convertDateToNumberOfDaysPassedFromEpoch(underTestDate);

    String formattedDate = converter.convert(epochDays);
    assertEquals(underTestDate, formattedDate);
  }

  @Test
  public void testMicroTimeConversion() {
    testMicroTimeConversionHelper(MICRO_TIMESTAMP, "22:20:38.808123");
    // Test case where microseconds have a leading 0.
    long microTimestamp = 1592511382050720L;
    testMicroTimeConversionHelper(microTimestamp, "20:16:22.050720");
  }

  private void testMicroTimeConversionHelper(long microTimestamp, String s) {
    MicroTimeConverter converter = new MicroTimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedMicroTime = converter.convert(microTimestamp);
    assertEquals(s, formattedMicroTime);
  }


  @Test
  public void testMicroTimestampConversion() {
    testMicroTimestampConversionHelper(MICRO_TIMESTAMP, "2017-03-01 22:20:38.808123");
    // Test timestamp where microseconds have a leading 0
    Long timestamp = 1592511382050720L;
    testMicroTimestampConversionHelper(timestamp, "2020-06-18 20:16:22.050720");
  }

  private void testMicroTimestampConversionHelper(Long timestamp, String s) {
    MicroTimestampConverter converter = new MicroTimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedMicroTimestamp = converter.convert(timestamp);
    assertEquals(s, formattedMicroTimestamp);
  }

  @Test
  public void testTimeConversion() {
    TimeConverter converter = new TimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTime = converter.convert(MILLI_TIMESTAMP_INT);
    assertEquals("05:26:46.838", formattedTime);
  }

  @Test
  public void testTimestampConversion() {
    TimestampConverter converter = new TimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTimestamp = converter.convert(MILLI_TIMESTAMP);
    assertEquals("2017-03-01 22:20:38.808", formattedTimestamp);
  }

  @Test
  public void testZonedTimestampConversion() {
    ZonedTimestampConverter converter = new ZonedTimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.STRING);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTimestamp = converter.convert("2017-03-01T14:20:38.808-08:00");
    assertEquals("2017-03-01 14:20:38.808-08:00", formattedTimestamp);
  }

  private int convertDateToNumberOfDaysPassedFromEpoch(String underTestDate) {
    // This is similar to conversion that happens in Debezium. For any date before 1970-01-01 the number of day is negative
    LocalDate date = LocalDate.parse(underTestDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    return (int) ChronoUnit.DAYS.between(EPOCH, date);
  }

}
