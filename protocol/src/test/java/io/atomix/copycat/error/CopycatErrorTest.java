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
    CopycatError error;
    error = CopycatError.forId(new NoLeaderException("test").getType().id());
    assertEquals(error, CopycatError.Type.NO_LEADER_ERROR);
    error = CopycatError.forId(new QueryException("test").getType().id());
    assertEquals(error, CopycatError.Type.QUERY_ERROR);
    error = CopycatError.forId(new CommandException("test").getType().id());
    assertEquals(error, CopycatError.Type.COMMAND_ERROR);
    error = CopycatError.forId(new ApplicationException("test").getType().id());
    assertEquals(error, CopycatError.Type.APPLICATION_ERROR);
    error = CopycatError.forId(new IllegalMemberStateException("test").getType().id());
    assertEquals(error, CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR);
    error = CopycatError.forId(new UnknownSessionException("test").getType().id());
    assertEquals(error, CopycatError.Type.UNKNOWN_SESSION_ERROR);
    error = CopycatError.forId(new InternalException("test").getType().id());
    assertEquals(error, CopycatError.Type.INTERNAL_ERROR);
    error = CopycatError.forId(new ConfigurationException("test").getType().id());
    assertEquals(error, CopycatError.Type.CONFIGURATION_ERROR);
  }

}
