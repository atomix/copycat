/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.copycat.client;

/**
 * Strategies for recovering lost client sessions.
 * <p>
 * Client recovery strategies are responsible for recovering a crashed client. When clients fail to contact
 * a server for more than their session timeout, the client's session must be closed as linearizability is
 * lost. The recovery strategy has the opportunity to recover the crashed client gracefully.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum RecoveryStrategies implements RecoveryStrategy {

  /**
   * The {@code CLOSE} strategy closes the client once its session is expired. A failure  will result in
   * exceptions for any commands submitted to the cluster. This ensures that users of the client cannot
   * operate on the cluster state while linearizability is lost.
   */
  CLOSE {
    @Override
    public void recover(CopycatClient client) {
      client.close();
    }
  },

  /**
   * The {@code RECOVER} strategy handles expired client sessions by recovering the session. Linearizable semantics
   * will be lost between commands across sessions.
   */
  RECOVER {
    @Override
    public void recover(CopycatClient client) {
      client.recover();
    }
  }

}
