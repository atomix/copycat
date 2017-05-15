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
package io.atomix.copycat.server.util;

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.protocol.Request;
import io.atomix.copycat.server.protocol.*;
import io.atomix.copycat.server.state.ServerMember;

import java.util.HashMap;
import java.util.Map;

/**
 * Request serializable type resolver.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ServerSerialization implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final Map<Class<? extends Request>, Integer> TYPES = new HashMap() {{
    put(AppendRequest.class, -21);
    put(ConfigureRequest.class, -22);
    put(InstallRequest.class, -23);
    put(JoinRequest.class, -24);
    put(LeaveRequest.class, -25);
    put(PollRequest.class, -26);
    put(ReconfigureRequest.class, -27);
    put(VoteRequest.class, -28);
    put(AppendResponse.class, -29);
    put(ConfigureResponse.class, -30);
    put(InstallResponse.class, -31);
    put(JoinResponse.class, -32);
    put(LeaveResponse.class, -33);
    put(PollResponse.class, -34);
    put(ReconfigureResponse.class, -35);
    put(VoteResponse.class, -36);
    put(ServerMember.class, -37);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    for (Map.Entry<Class<? extends Request>, Integer> entry : TYPES.entrySet()) {
      registry.register(entry.getKey(), entry.getValue());
    }
  }

}
