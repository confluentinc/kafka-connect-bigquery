package com.wepay.kafka.connect.bigquery.integration.utils;

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


import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GCSBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketClearer {

  private static final Logger logger = LoggerFactory.getLogger(BucketClearer.class);

  /**
   * Clear out a GCS bucket. Useful in integration testing to provide a clean slate before creating
   * a connector and writing to that bucket.
   * @param key The GCP credentials to use (can be a filename or a raw JSON string).
   * @param project The GCP project the bucket belongs to.
   * @param bucket The bucket to clear.
   * @param keySource The key source. If "FILE", then the {@code key} parameter will be treated as a
   *                  filename; if "JSON", then {@code key} will be treated as a raw JSON string.
   */
  public static void clearBucket(String key, String project, String bucket, String keySource) {
    Storage gcs = new GCSBuilder(project).setKey(key).setKeySource(keySource).build();
    if (gcs.delete(bucket)) {
      logger.info("Bucket {} deleted successfully", bucket);
    } else {
      logger.info("Bucket {} does not exist", bucket);
    }
  }
}
