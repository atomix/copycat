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
package io.atomix.copycat.protocol.tcp;

import io.atomix.copycat.protocol.ssl.SslOptions;
import io.atomix.copycat.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * Netty TLS.
 *
 * @author <a href="http://github.com/electrical">Richard Pijnenburg</a>
 */
public final class NettyTcpTls {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpTls.class);
  private SslOptions options;

  public NettyTcpTls(SslOptions options) {
    this.options = options;
  }

  /**
   * Initializes an SSL engine.
   *
   * @param client Indicates whether the engine is being initialized for a client.
   * @return The initialized SSL engine.
   */
  public SSLEngine initSslEngine(boolean client) throws Exception {
    // Load the keystore
    KeyStore keyStore = loadKeystore(options.sslKeyStorePath(), options.sslKeyStorePassword());

    // Setup the keyManager to use our keystore
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, keyStoreKeyPass(options));

    // Setup the Trust keystore
    KeyStore trustStore;
    if (options.sslTrustStorePath() != null) {
      // Use the separate Trust keystore
      LOGGER.debug("Using separate trust store");
      trustStore = loadKeystore(options.sslTrustStorePath(), options.sslTrustStorePassword());
    } else {
      // Reuse the existing keystore
      trustStore = keyStore;
      LOGGER.debug("Using key store as trust store");
    }

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagers, null);
    SSLEngine sslEngine = sslContext.createSSLEngine();
    sslEngine.setUseClientMode(client);
    sslEngine.setWantClientAuth(true);
    sslEngine.setEnabledProtocols(sslEngine.getSupportedProtocols());
    sslEngine.setEnabledCipherSuites(sslEngine.getSupportedCipherSuites());
    sslEngine.setEnableSessionCreation(true);

    return sslEngine;
  }

  /**
   * Loads the key store.
   */
  private KeyStore loadKeystore(String path, String password) throws Exception {
    Assert.notNull(path, "Path");
    File file = new File(path);

    LOGGER.debug("Using JKS at {}", file.getCanonicalPath());
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(new FileInputStream(file.getCanonicalPath()), password.toCharArray());
    return ks;
  }

  /**
   * Returns the key store password.
   */
  private char[] keyStoreKeyPass(SslOptions options) throws Exception {
    if (options.sslKeyStoreKeyPassword() != null) {
      return options.sslKeyStoreKeyPassword().toCharArray();
    } else {
      return options.sslKeyStorePassword().toCharArray();
    }
  }
}
