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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_DEFAULT;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_TYPES;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

public abstract class GcpClientBuilder<Client> {

  public enum KeySource {
    FILE, JSON, APPLICATION_DEFAULT
  }

  private static final Logger logger = LoggerFactory.getLogger(GcpClientBuilder.class);
  private static final String USER_AGENT_HEADER_KEY = "user-agent";
  private static final String USER_AGENT_HEADER_FORMAT = "%s (GPN: Confluent;) Google BigQuery Sink/%s";
  private HeaderProvider headerProvider = null;
  private String project = null;
  private KeySource keySource = null;
  private String key = null;


  private boolean useStorageWriteApi = false;

  // Scope list taken from : https://developers.google.com/identity/protocols/oauth2/scopes#bigquery
  private static final Collection<String> scopes = Lists.newArrayList(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/bigquery.insertdata",
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/cloud-platform.read-only",
          "https://www.googleapis.com/auth/devstorage.full_control",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/devstorage.read_write"
  );

  public GcpClientBuilder<Client> withConfig(BigQuerySinkConfig config) {
    return withProject(config.getString(PROJECT_CONFIG))
        .withKeySource(config.getKeySource())
        .withKey(config.getKey())
        .withUserAgent(config.getString(CONNECTOR_RUNTIME_PROVIDER_CONFIG))
        .withWriterApi(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG));
  }

  public GcpClientBuilder<Client> withProject(String project) {
    Objects.requireNonNull(project, "Project cannot be null");
    this.project = project;
    return this;
  }

  public GcpClientBuilder<Client> withWriterApi(Boolean useStorageWriteApi) {
    this.useStorageWriteApi = useStorageWriteApi;
    return this;
  }

  public GcpClientBuilder<Client> withKeySource(KeySource keySource) {
    Objects.requireNonNull(keySource, "Key cannot be null");
    this.keySource = keySource;
    return this;
  }

  public GcpClientBuilder<Client> withKey(String key) {
    this.key = key;
    return this;
  }

  public GcpClientBuilder<Client> withUserAgent(String userAgent) {
    if (!CONNECTOR_RUNTIME_PROVIDER_TYPES.contains(userAgent)) {
      logger.warn(String.format("Invalid Kafka runtime provider value received. Provider : %s. Defaulting to %s",
              userAgent, CONNECTOR_RUNTIME_PROVIDER_DEFAULT));
      userAgent = CONNECTOR_RUNTIME_PROVIDER_DEFAULT;
    }
    this.headerProvider = FixedHeaderProvider.create(USER_AGENT_HEADER_KEY,
            String.format(USER_AGENT_HEADER_FORMAT, userAgent, Version.version()));
    return this;
  }

  public Client build() {
    return doBuild(project, credentials(), headerProvider);
  }

  private GoogleCredentials credentials() {
    if (key == null && keySource != KeySource.APPLICATION_DEFAULT) {
      return null;
    }

    Objects.requireNonNull(keySource, "Key source must be defined to build a GCP client");
    Objects.requireNonNull(project, "Project must be defined to build a GCP client");

    InputStream credentialsStream;
    switch (keySource) {
      case JSON:
        credentialsStream = new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8));
        break;
      case FILE:
        try {
          logger.debug("Attempting to open file {} for service account json key", key);
          credentialsStream = new FileInputStream(key);
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to access JSON key file", e);
        }
        break;
      case APPLICATION_DEFAULT:
        try {
          logger.debug("Attempting to use application default credentials");
          return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to create Application Default Credentials", e);
        }
      default:
        throw new IllegalArgumentException("Unexpected value for KeySource enum: " + keySource);
    }

    try {
      return useStorageWriteApi ?
              GoogleCredentials.fromStream(credentialsStream).createScoped(scopes)
              : GoogleCredentials.fromStream(credentialsStream);
    } catch (IOException e) {
      throw new BigQueryConnectException("Failed to create credentials from input stream", e);
    }
  }

  protected abstract Client doBuild(String project, GoogleCredentials credentials, HeaderProvider userAgent);

  public static class BigQueryBuilder extends GcpClientBuilder<BigQuery> {
    @Override
    protected BigQuery doBuild(String project, GoogleCredentials credentials, HeaderProvider headerProvider) {
      BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
          .setProjectId(project)
          .setHeaderProvider(headerProvider);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access BigQuery without authentication");
      }

      return builder.build().getService();
    }
  }

  public static class GcsBuilder extends GcpClientBuilder<Storage> {
    @Override
    protected Storage doBuild(String project, GoogleCredentials credentials, HeaderProvider headerProvider) {
      StorageOptions.Builder builder = StorageOptions.newBuilder()
          .setProjectId(project)
          .setHeaderProvider(headerProvider);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access GCS without authentication");
      }

      return builder.build().getService();
    }
  }

  @VisibleForTesting
  HeaderProvider getHeaderProvider() {
    return this.headerProvider;
  }
}
