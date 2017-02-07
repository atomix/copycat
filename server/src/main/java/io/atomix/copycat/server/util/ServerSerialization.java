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
    put(AppendRequest.class, -18);
    put(ConfigureRequest.class, -19);
    put(InstallRequest.class, -20);
    put(JoinRequest.class, -21);
    put(LeaveRequest.class, -22);
    put(PollRequest.class, -23);
    put(ReconfigureRequest.class, -24);
    put(VoteRequest.class, -25);
    put(AppendResponse.class, -27);
    put(ConfigureResponse.class, -28);
    put(InstallResponse.class, -29);
    put(JoinResponse.class, -30);
    put(LeaveResponse.class, -31);
    put(PollResponse.class, -32);
    put(ReconfigureResponse.class, -33);
    put(VoteResponse.class, -34);
    put(ServerMember.class, -35);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    for (Map.Entry<Class<? extends Request>, Integer> entry : TYPES.entrySet()) {
      registry.register(entry.getKey(), entry.getValue());
    }
  }

}
