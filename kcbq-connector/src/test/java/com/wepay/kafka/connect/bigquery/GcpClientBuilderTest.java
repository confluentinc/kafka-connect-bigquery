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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GcpClientBuilderTest {

  private static final String DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token";
  private static final String CLIENT_EMAIL = "test@test-project.iam.gserviceaccount.com";

  /**
   * Test double that exposes the credentials produced by {@link GcpClientBuilder#credentials()}
   * (otherwise private) without constructing a real GCP client.
   */
  private static class CapturingBuilder extends GcpClientBuilder<GoogleCredentials> {
    @Override
    protected GoogleCredentials doBuild(String project, GoogleCredentials credentials,
                                        HeaderProvider userAgent) {
      return credentials;
    }
  }

  private GoogleCredentials buildCredentials(GcpClientBuilder.KeySource keySource, String key) {
    return new CapturingBuilder()
        .withProject("test-project")
        .withKeySource(keySource)
        .withKey(key)
        .build();
  }

  @Test
  public void testJsonKeySourceOverridesAttackerControlledTokenUri() {
    String key = serviceAccountJson("https://attacker.example.com/steal-token");

    GoogleCredentials credentials = buildCredentials(GcpClientBuilder.KeySource.JSON, key);

    assertTrue(credentials instanceof ServiceAccountCredentials);
    ServiceAccountCredentials serviceAccount = (ServiceAccountCredentials) credentials;
    // The malicious token_uri from the JSON must be discarded in favour of Google's default,
    // otherwise token requests could be redirected to an attacker-controlled endpoint (SSRF).
    assertEquals(DEFAULT_TOKEN_URI, serviceAccount.getTokenServerUri().toString());
    assertEquals(CLIENT_EMAIL, serviceAccount.getClientEmail());
  }

  @Test
  public void testJsonKeySourceWithoutTokenUriUsesDefault() {
    String key = serviceAccountJson(null);

    GoogleCredentials credentials = buildCredentials(GcpClientBuilder.KeySource.JSON, key);

    assertTrue(credentials instanceof ServiceAccountCredentials);
    assertEquals(DEFAULT_TOKEN_URI,
        ((ServiceAccountCredentials) credentials).getTokenServerUri().toString());
  }

  @Test
  public void testFileKeySourceParsesCredentialsAndOverridesTokenUri() throws Exception {
    File keyFile = File.createTempFile("kcbq-credentials", ".json");
    keyFile.deleteOnExit();
    Files.write(keyFile.toPath(),
        serviceAccountJson("https://attacker.example.com/steal-token")
            .getBytes(StandardCharsets.UTF_8));

    GoogleCredentials credentials =
        buildCredentials(GcpClientBuilder.KeySource.FILE, keyFile.getAbsolutePath());

    assertTrue(credentials instanceof ServiceAccountCredentials);
    assertEquals(DEFAULT_TOKEN_URI,
        ((ServiceAccountCredentials) credentials).getTokenServerUri().toString());
  }

  @Test
  public void testInvalidJsonThrowsBigQueryConnectException() {
    assertThrows(
        BigQueryConnectException.class,
        () -> buildCredentials(GcpClientBuilder.KeySource.JSON, "not valid json"));
  }

  @Test
  public void testNullKeyYieldsNullCredentials() {
    assertNull(buildCredentials(GcpClientBuilder.KeySource.JSON, null));
  }

  /**
   * Builds a minimal service account credential JSON with a freshly generated RSA private key
   * (required because {@link ServiceAccountCredentials#fromStream} actually parses the key).
   *
   * @param tokenUri value for the {@code token_uri} field, or {@code null} to omit it
   */
  private static String serviceAccountJson(String tokenUri) {
    String privateKeyPem = generatePrivateKeyPem().replace("\n", "\\n");
    StringBuilder json = new StringBuilder()
        .append("{\n")
        .append("  \"type\": \"service_account\",\n")
        .append("  \"project_id\": \"test-project\",\n")
        .append("  \"private_key_id\": \"test-key-id\",\n")
        .append("  \"private_key\": \"").append(privateKeyPem).append("\",\n")
        .append("  \"client_email\": \"").append(CLIENT_EMAIL).append("\",\n")
        .append("  \"client_id\": \"123456789\"");
    if (tokenUri != null) {
      json.append(",\n  \"token_uri\": \"").append(tokenUri).append("\"");
    }
    return json.append("\n}").toString();
  }

  private static String generatePrivateKeyPem() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair keyPair = generator.generateKeyPair();
      String base64 = Base64.getMimeEncoder(64, new byte[] {'\n'})
          .encodeToString(keyPair.getPrivate().getEncoded());
      return "-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n";
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate RSA key for test", e);
    }
  }
}
