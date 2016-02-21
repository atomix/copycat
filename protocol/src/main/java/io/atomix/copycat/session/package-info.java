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

/**
 * Classes and interfaces for managing client sessions.
 * <p>
 * Sessions represent the context in which clients communicate with a Raft cluster. Sessions allow clusters
 * to process requests according to client state. For instance, sessions are responsible for ensuring operations
 * are executed in the order specified by the client (sequential consistency) and operations are only applied
 * to the replicated state machine once (linearizability).
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.copycat.session;
