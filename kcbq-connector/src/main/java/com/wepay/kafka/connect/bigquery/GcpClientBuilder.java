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
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_DEFAULT;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_TYPES;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;

public abstract class GcpClientBuilder<Client> {

  public enum KeySource {
    FILE, JSON, APPLICATION_DEFAULT
  }

  private static final Logger logger = LoggerFactory.getLogger(GcpClientBuilder.class);
  private static final String USER_AGENT_HEADER_KEY = "user-agent";
  private static final String USER_AGENT_HEADER_FORMAT = "%s (GPN: Confluent;) Google BigQuery Sink/%s";
  // ServiceAccountCredentials.fromStream() honors a token_uri field from the input JSON, so we
  // override it with Google's default endpoint to prevent SSRF via attacker-controlled token URIs.
  private static final URI DEFAULT_TOKEN_SERVER_URI =
      URI.create("https://oauth2.googleapis.com/token");
  private HeaderProvider headerProvider = null;
  private String project = null;
  private KeySource keySource = null;
  private String key = null;

  public GcpClientBuilder<Client> withConfig(BigQuerySinkConfig config) {
    return withProject(config.getString(PROJECT_CONFIG))
        .withKeySource(config.getKeySource())
        .withKey(config.getKey())
        .withUserAgent(config.getString(CONNECTOR_RUNTIME_PROVIDER_CONFIG));
  }

  public GcpClientBuilder<Client> withProject(String project) {
    Objects.requireNonNull(project, "Project cannot be null");
    this.project = project;
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

    try {
      switch (keySource) {
        case JSON:
          logger.debug("Attempting to authenticate with BigQuery using json key");
          try (InputStream stream =
                   new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8))) {
            return credentialsFromStream(stream);
          }
        case FILE:
          logger.debug("Attempting to authenticate with BigQuery using credentials file");
          try (InputStream stream = new FileInputStream(key)) {
            return credentialsFromStream(stream);
          }
        case APPLICATION_DEFAULT:
          logger.debug("Attempting to use application default credentials");
          return GoogleCredentials.getApplicationDefault();
        default:
          throw new IllegalArgumentException("Unexpected value for KeySource enum: " + keySource);
      }
    } catch (IOException e) {
      throw new BigQueryConnectException("Failed to create credentials", e);
    }
  }

  /**
   * Parse GCP service account credentials from the given stream, pinning the token server URI to
   * Google's default endpoint. Using the type-specific {@link ServiceAccountCredentials#fromStream}
   * loader (rather than filtering fields manually) and overriding {@code token_uri} prevents SSRF
   * via attacker-controlled token endpoints in the supplied credential JSON.
   */
  private static GoogleCredentials credentialsFromStream(InputStream stream) throws IOException {
    return ServiceAccountCredentials.fromStream(stream)
        .toBuilder()
        .setTokenServerUri(DEFAULT_TOKEN_SERVER_URI)
        .build();
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
