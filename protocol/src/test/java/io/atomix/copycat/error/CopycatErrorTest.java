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
package io.atomix.copycat.error;

import io.atomix.copycat.protocol.error.*;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Copycat error serialization test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class CopycatErrorTest {

  /**
   * Tests serializing exceptions.
   */
  public void testSerializeErrors() throws Throwable {
    ProtocolResponse.Error.Type error;
    error = ProtocolResponse.Error.Type.forId(new NoLeaderException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.NO_LEADER_ERROR);
    error = ProtocolResponse.Error.Type.forId(new QueryException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.QUERY_ERROR);
    error = ProtocolResponse.Error.Type.forId(new CommandException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.COMMAND_ERROR);
    error = ProtocolResponse.Error.Type.forId(new ApplicationException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.APPLICATION_ERROR);
    error = ProtocolResponse.Error.Type.forId(new IllegalMemberStateException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR);
    error = ProtocolResponse.Error.Type.forId(new UnknownSessionException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.UNKNOWN_SESSION_ERROR);
    error = ProtocolResponse.Error.Type.forId(new InternalException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.INTERNAL_ERROR);
    error = ProtocolResponse.Error.Type.forId(new ConfigurationException("test").getType().id());
    assertEquals(error, ProtocolResponse.Error.Type.CONFIGURATION_ERROR);
  }

}
