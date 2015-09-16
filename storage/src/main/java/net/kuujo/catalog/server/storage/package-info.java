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
 * Provides a standalone segmented log for Catalog's <a href="https://raftconsensus.github.io/">Raft</a> implementation.
 * <p>
 * The log is designed as a standalone journal built on Catalog's {@link net.kuujo.catalyst.buffer.Buffer} abstraction.
 * The buffer abstraction allows Catalog's {@link net.kuujo.catalog.server.storage.Log} to write to memory or disk based on the
 * buffer type.
 * <p>
 * While the log is not dependent on the Raft algorithm, it does implement many features in support of the Raft implementation.
 * Specifically, the log is not an append-only log. Rather, it supports appending to, truncating, and compacting the
 * log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package net.kuujo.catalog.server.storage;
