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
package io.atomix.copycat.client;

/**
 * Typed command.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface TypedCommand<T> extends Command<T> {

  /**
   * Constants for specifying the command type.
   *
   * @see #type()
   */
  enum Type {

    /**
     * The {@code DEFAULT} command type is a special command type that behaves differently according
     * to whether the system supports snapshots.
     */
    DEFAULT {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.DEFAULT;
      }
    },

    /**
     * The {@code UNKNOWN} command type is a special command type that covers all use cases for all
     * state machines but may result in inefficient log compaction.
     */
    UNKNOWN {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.UNKNOWN;
      }
    },

    /**
     * The {@code UPDATE} command type indicates that the command is used to update system state and may
     * be retained in the state machine for an arbitrary amount of time.
     */
    UPDATE {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.QUORUM;
      }
    },

    /**
     * The {@code EXPIRING} command type indicates that the command is used to update system state but may
     * ultimately expire after some arbitrary amount of time after which it will no longer contribute to
     * the system's state.
     */
    EXPIRING {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.SEQUENTIAL;
      }
    },

    /**
     * The {@code DELETE} command type indicates that the command is used to delete system state.
     */
    DELETE {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.SEQUENTIAL;
      }
    },

    /**
     * The {@code EVENT} command type is a special type used to indicate that the command triggers the
     * publishing of events but does not itself contribute to the system's state.
     */
    EVENT {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.QUORUM;
      }
    };

    /**
     * Returns the compaction mode associated with this command type.
     *
     * @return The compaction mode associated with this command type.
     */
    public abstract CompactionMode compaction();

  }

  /**
   * Returns the command type.
   * <p>
   * The command type indicates how servers should handle persistence and replication of the command. Different
   * types of commands can be optimized based on the way they modify system state.
   *
   * @return The command type.
   */
  default Type type() {
    return Type.DEFAULT;
  }

  /**
   * Returns the command compaction mode.
   * <p>
   * The compaction mode will dictate how long the command is persisted in the Raft log before it can be compacted.
   *
   * @see CompactionMode
   *
   * @return The command compaction mode.
   */
  default CompactionMode compaction() {
    return type().compaction();
  }

}
