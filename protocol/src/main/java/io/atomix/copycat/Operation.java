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
 * limitations under the License.
 */
package io.atomix.copycat;

import io.atomix.copycat.session.Session;

import java.io.Serializable;

/**
 * Base type for Raft state operations.
 * <p>
 * This is a base interface for operations on the Raft cluster state. Operations are submitted to Raft clusters
 * by clients via a {@link Session}. All operations are sent over the network
 * and thus must be serializable by the client and by all servers in the cluster. By default, Java serialization
 * is used. However, it is recommended that operations implement {@link io.atomix.catalyst.serializer.CatalystSerializable}
 * or register a {@link io.atomix.catalyst.serializer.TypeSerializer} for better performance.
 *
 * @see Command
 * @see Query
 *
 * @param <T> operation result type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Operation<T> extends Serializable {
}
