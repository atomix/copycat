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
 * Strategy for recovering client sessions on failures.
 * <p>
 * Client recovery strategies are responsible for recovering a crashed client. When a client is unable
 * to communicate with the cluster for some time period, the cluster may expire the client's session.
 * In the event that a client reconnects and discovers its session is expired, the client's configured
 * recovery strategy will be queried to determine how to handle the failure. Typically, recovery strategies
 * can either {@link CopycatClient#recover() recover} or {@link CopycatClient#close() close} the client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RecoveryStrategy {

  /**
   * Recovers the client.
   *
   * @param client The recoverable client.
   */
  void recover(CopycatClient client);

}
