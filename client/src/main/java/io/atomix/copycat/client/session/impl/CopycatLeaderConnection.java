/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.session.impl;

import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader connection.
 */
public class CopycatLeaderConnection extends CopycatConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopycatLeaderConnection.class);
  private final String sessionString;

  public CopycatLeaderConnection(CopycatSessionState state, ClientConnectionManager connections, AddressSelector selector) {
    super(connections, selector);
    this.sessionString = String.valueOf(state.getSessionId());
  }

  @Override
  protected String name() {
    return sessionString;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

}
