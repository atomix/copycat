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
package io.atomix.copycat.server.storage.compaction;

/**
 * Log compaction type identifier.
 * <p>
 * Log compaction comes in two forms: {@link #MINOR} and {@link #MAJOR}. This enum provides identifiers
 * to indicate the log compaction type when {@link Compactor#compact(Compaction) compacting} the log.
 * Passing an explicit {@code Compaction} type will run the appropriate compaction process asynchronously.
 * <pre>
 *   {@code
 *   log.compactor().compact(Compaction.MAJOR);
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum Compaction {

  /**
   * Represents a minor compaction.
   * <p>
   * Minor compaction is the more efficient of the compaction processes which removes
   * {@link io.atomix.copycat.server.storage.Log#clean(long) cleaned} non-tombstone entries from individual
   * {@link io.atomix.copycat.server.storage.Segment}s. See the {@link MinorCompactionTask} for more information.
   */
  MINOR {
    @Override
    CompactionManager manager() {
      return new MinorCompactionManager();
    }
  },

  /**
   * Represents a major compaction.
   * <p>
   * Major compaction is the more heavyweight process of removing all
   * {@link io.atomix.copycat.server.storage.Log#clean(long) cleaned} entries that have been
   * {@link io.atomix.copycat.server.storage.Log#commit(long) committed} to the log and combining segment
   * files wherever possible. See the {@link MajorCompactionTask} for more information.
   */
  MAJOR {
    @Override
    CompactionManager manager() {
      return new MajorCompactionManager();
    }
  };

  /**
   * Returns the compaction manager for the compaction type.
   *
   * @return The compaction manager for the compaction type.
   */
  abstract CompactionManager manager();

}
