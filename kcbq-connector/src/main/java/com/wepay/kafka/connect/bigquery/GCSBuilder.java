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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.wepay.kafka.connect.bigquery.exception.GCSConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Convenience class for creating a {@link com.google.cloud.storage.Storage} instance
 */
public class GCSBuilder {
  private static final Logger logger = LoggerFactory.getLogger(GCSBuilder.class);

  private final String projectName;
  private String keyFileName;
  private Boolean isJsonString;

  public GCSBuilder(String projectName) {
    this.projectName = projectName;
    this.keyFileName = null;
  }

  public GCSBuilder setKeyFileName(String keyFileName, Boolean isJsonString) {
    this.keyFileName = keyFileName;
    this.isJsonString = isJsonString;
    return this;
  }

  public Storage build() {
    return connect(projectName, keyFileName, isJsonString);
  }

  /**
   * Returns a default {@link Storage} instance for the specified project with credentials provided
   * in the specified file.
   *
   * @param projectName The name of the GCS project to work with
   * @param keyFilename The name of a file containing a JSON key that can be used to provide
   *                    credentials to GCS, or null if no authentication should be performed.
   * @return The resulting Storage object.
   */
  private Storage connect(String projectName, String keyFilename) {
    if (keyFilename == null) {
      return connect(projectName);
    }

    logger.debug("Attempting to open file {} for service account json key", keyFilename);
    try (InputStream credentialsStream = new FileInputStream(keyFilename)) {
      logger.debug("Attempting to authenticate with GCS using provided json key");
      return StorageOptions.newBuilder()
          .setProjectId(projectName)
          .setCredentials(GoogleCredentials.fromStream(credentialsStream))
          .build()
          .getService();
    } catch (IOException err) {
      throw new GCSConnectException("Failed to access json key file", err);
    }
  }

  /**
   * Returns a default {@link Storage} instance for the specified project with credentials provided
   * in the specified file, which can then be used for creating, updating, and inserting into tables
   * from specific datasets.
   *
   * @param projectName The name of the BigQuery project to work with
   * @param keyFile The name of a file containing a JSON key that can be used to provide
   *                    credentials to BigQuery, or null if no authentication should be performed.
   * @param isJsonString If isJsonString is set to true, the method will interpret the keyFile as
   *                     JsonString instead of file path
   * @return The resulting BigQuery object.
   */
  private Storage connect(String projectName, String keyFile, Boolean isJsonString) {
    if (!isJsonString) {
      return connect(projectName, keyFile);
    }

    logger.debug("Attempting to convert json key string to input stream");
    try (InputStream credentialsStream = new ByteArrayInputStream(keyFile.getBytes())) {
      logger.debug("Attempting to authenticate with GCS using provided json key");
      return StorageOptions.newBuilder()
              .setProjectId(projectName)
              .setCredentials(GoogleCredentials.fromStream(credentialsStream))
              .build()
              .getService();
    } catch (IOException err) {
      throw new GCSConnectException("Failed to read json key string", err);
    }
  }

  /**
   * Returns a default {@link Storage} instance for the specified project with no authentication
   * credentials.
   *
   * @param projectName The name of the GCS project to work with
   * @return The resulting Storage object.
   */
  private Storage connect(String projectName) {
    logger.debug("Attempting to access BigQuery without authentication");
    return StorageOptions.newBuilder()
        .setProjectId(projectName)
        .build()
        .getService();
  }
}
