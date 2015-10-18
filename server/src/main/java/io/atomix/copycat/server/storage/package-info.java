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
 * Provides a standalone segmented log for Copycat's <a href="https://raftconsensus.github.io/">Raft</a> implementation.
 * <p>
 * Logs are the vehicle through which Copycat servers persist and replicate state changes. The Copycat log is designed as
 * a standalone journal built on Copycat's {@link io.atomix.catalyst.buffer.Buffer} abstraction. Entries are written to the
 * log in many separate buffers known as segments. The buffer abstraction allows Copycat's
 * {@link io.atomix.copycat.server.storage.Log} to write to memory or disk based on the buffer type.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.copycat.server.storage;
