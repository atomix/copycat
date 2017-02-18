/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.protocol.ssl;

import io.atomix.copycat.util.Assert;

/**
 * SSL options.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class SslOptions {

  /**
   * Returns a new Netty TCP options builder.
   *
   * @return A new Netty TCP options builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final boolean DEFAULT_SSL_ENABLED = false;
  private static final SslProtocol DEFAULT_SSL_PROTOCOL = SslProtocol.TLSv1_2;

  private boolean enabled = DEFAULT_SSL_ENABLED;
  private SslProtocol protocol = DEFAULT_SSL_PROTOCOL;
  private String keyStorePath;
  private String keyStorePassword;
  private String trustStorePath;
  private String trustStorePassword;

  /**
   * Whether SSL is enabled.
   */
  public boolean enabled() {
    return enabled;
  }

  /**
   * The SSL Protocol.
   */
  public SslProtocol protocol() {
    return protocol;
  }

  /**
   * The SSL trust store path.
   */
  public String sslTrustStorePath() {
    return trustStorePath;
  }

  /**
   * The SSL trust store password.
   */
  public String sslTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * The SSL key store path.
   */
  public String sslKeyStorePath() {
    return keyStorePassword;
  }

  /**
   * The SSL key store password.
   */
  public String sslKeyStorePassword() {
    return keyStorePath;
  }

  /**
   * The SSL key store key password.
   */
  public String sslKeyStoreKeyPassword() {
    return keyStorePassword;
  }

  /**
   * SSL options builder.
   */
  public static class Builder implements io.atomix.copycat.util.Builder<SslOptions> {
    private final SslOptions options = new SslOptions();

    private Builder() {
    }

    /**
     * Sets whether SSL is enabled.
     *
     * @param enabled Whether SSL is enabled.
     * @return The SSL options builder.
     */
    public Builder withEnabled(boolean enabled) {
      options.enabled = enabled;
      return this;
    }

    /**
     * Sets the SSL protocol.
     *
     * @param sslProtocol The SSL protocol.
     * @return The SSL options builder.
     */
    public Builder withSslProtocol(SslProtocol sslProtocol) {
      options.protocol = Assert.notNull(sslProtocol, "sslProtocol");
      return this;
    }

    /**
     * Sets the SSL trust store path.
     *
     * @param trustStorePath The trust store path.
     * @return The SSL options builder.
     */
    public Builder withTrustStorePath(String trustStorePath) {
      options.trustStorePath = Assert.notNull(trustStorePath, "trustStorePath");
      return this;
    }

    /**
     * Sets the SSL trust store password.
     *
     * @param trustStorePassword The trust store password.
     * @return The SSL options builder.
     */
    public Builder withTrustStorePassword(String trustStorePassword) {
      options.trustStorePassword = Assert.notNull(trustStorePassword, "trustStorePassword");
      return this;
    }

    /**
     * Sets the SSL key store path.
     *
     * @param keyStorePath The key store path.
     * @return The SSL options builder.
     */
    public Builder withKeyStorePath(String keyStorePath) {
      options.keyStorePath = Assert.notNull(keyStorePath, "keyStorePath");
      return this;
    }

    /**
     * Sets the SSL key store password.
     *
     * @param keyStorePassword The key store password.
     * @return The SSL options builder.
     */
    public Builder withKeyStorePassword(String keyStorePassword) {
      options.keyStorePassword = Assert.notNull(keyStorePassword, "keyStorePassword");
      return this;
    }


    @Override
    public SslOptions build() {
      return options;
    }
  }

}
