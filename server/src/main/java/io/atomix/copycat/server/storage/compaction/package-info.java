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

/**
 * Classes and interfaces that facilitate compaction of the Copycat {@link io.atomix.copycat.server.storage.Log log}.
 * <p>
 * The log compaction package implements compaction for Copycat {@link io.atomix.copycat.server.storage.Log logs} using
 * a custom log cleaning algorithm. As entries are written to the log and applied to the server's state machine, the
 * state machine can arbitrarily mark entries for removal from the log. Periodically, a set of log compaction threads
 * will compact {@link io.atomix.copycat.server.storage.Segment segment}s of the log in the background. Log compaction
 * is performed in two phases: {@link io.atomix.copycat.server.storage.compaction.MinorCompactionTask minor} and
 * {@link io.atomix.copycat.server.storage.compaction.MajorCompactionTask major}. The minor compaction process efficiently
 * rewrites individual segments to remove standard entries that have been marked for cleaning. The major compaction process
 * periodically rewrites the entire log to combine segments that have previously been compacted.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.copycat.server.storage.compaction;
