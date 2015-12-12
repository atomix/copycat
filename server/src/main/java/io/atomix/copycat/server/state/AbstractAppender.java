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
package io.atomix.copycat.server.state;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.ConfigureRequest;
import io.atomix.copycat.server.request.InstallRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.ConfigureResponse;
import io.atomix.copycat.server.response.InstallResponse;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Entries appenders handle sending {@link AppendRequest}s from leaders to followers and from
 * followers to passive/reserve members.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
abstract class AbstractAppender implements AutoCloseable {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerState context;
  private final Set<MemberState> appending = new HashSet<>();
  private final Set<MemberState> configuring = new HashSet<>();
  private final Set<MemberState> installing = new HashSet<>();
  private boolean open = true;

  AbstractAppender(ServerState context) {
    this.context = context;
  }

  /**
   * Sends a configuration to the given member.
   */
  protected void configure(MemberState member) {
    if (!configuring.contains(member)) {
      ConfigureRequest request = buildConfigureRequest(member);
      if (request != null) {
        sendConfigureRequest(member, request);
      }
    }
  }

  /**
   * Connects to the member and sends a configure request.
   */
  protected void sendConfigureRequest(MemberState member, ConfigureRequest request) {
    configuring.add(member);

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, request);
        } else {
          configuring.remove(member);
        }
      }
    });
  }

  /**
   * Sends a configuration message.
   */
  protected void sendConfigureRequest(Connection connection, MemberState member, ConfigureRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    connection.<ConfigureRequest, ConfigureResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();
      configuring.remove(member);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          member.setConfigTerm(request.term()).setConfigIndex(request.index());
          appendEntries(member);
        } else {
          LOGGER.warn("{} - Failed to configure {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());
        }
      }
    });
  }

  /**
   * Sends a snapshot to the given member.
   */
  protected void install(MemberState member) {
    if (!installing.contains(member)) {
      InstallRequest request = buildInstallRequest(member);
      if (request != null) {
        sendInstallRequest(member, request);
      }
    }
  }

  /**
   * Connects to the member and sends a snapshot request.
   */
  protected void sendInstallRequest(MemberState member, InstallRequest request) {
    installing.add(member);

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendInstallRequest(connection, member, request);
        } else {
          installing.remove(member);
        }
      }
    });
  }

  /**
   * Sends a snapshot message.
   */
  protected void sendInstallRequest(Connection connection, MemberState member, InstallRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    connection.<InstallRequest, InstallResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();
      installing.remove(member);

      if (open) {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());

            // If the install request was completed successfully, set the member's sapshotIndex.
            if (request.complete()) {
              member.setSnapshotIndex(request.index());
              member.setNextSnapshotIndex(0);
              member.setNextSnapshotOffset(0);
            } else {
              member.setNextSnapshotOffset(member.getNextSnapshotOffset() + 1);
            }

            appendEntries(member);
          } else {
            LOGGER.warn("{} - Failed to install {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());
            member.setNextSnapshotIndex(0);
            member.setNextSnapshotOffset(0);
          }
        } else {
          LOGGER.warn("{} - Failed to install {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());
          member.setNextSnapshotIndex(0);
          member.setNextSnapshotOffset(0);
        }
      }
    });
  }

  /**
   * Sends append entries requests to the given member.
   */
  protected void appendEntries(MemberState member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open)
      return;

    // If the member term is less than the current term or the member's configuration index is less
    // than the local configuration index, send a configuration update to the member.
    // Ensure that only one configuration attempt per member is attempted at any given time by storing the
    // member state in a set of configuring members.
    // Once the configuration is complete sendAppendRequest will be called recursively.
    if (member.getConfigTerm() < context.getTerm() || member.getConfigIndex() < context.getCluster().getVersion()) {
      configure(member);
    }
    // If the member's current snapshot index is less than the latest snapshot index and the latest snapshot index
    // is less than the nextIndex, send a snapshot request.
    else if (context.getSnapshotStore().currentSnapshot() != null
      && context.getSnapshotStore().currentSnapshot().index() >= member.getNextIndex()
      && context.getSnapshotStore().currentSnapshot().index() > member.getSnapshotIndex()) {
      install(member);
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (!appending.contains(member)) {
      AppendRequest request = buildAppendRequest(member);
      if (request != null) {
        sendAppendRequest(member, request);
      }
    }
  }

  /**
   * Builds a configure request for the given member.
   */
  protected ConfigureRequest buildConfigureRequest(MemberState member) {
    Member leader = context.getLeader();
    return ConfigureRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withIndex(context.getCluster().getVersion())
      .withMembers(context.getCluster().getMembers())
      .build();
  }

  /**
   * Builds an install request for the given member.
   */
  protected InstallRequest buildInstallRequest(MemberState member) {
    Snapshot snapshot = context.getSnapshotStore().currentSnapshot();
    if (member.getNextSnapshotIndex() != snapshot.index()) {
      member.setNextSnapshotIndex(snapshot.index()).setNextSnapshotOffset(0);
    }

    InstallRequest request;
    synchronized (snapshot) {
      // Open a new snapshot reader.
      try (SnapshotReader reader = snapshot.reader()) {
        // Skip to the next batch of bytes according to the snapshot chunk size and current offset.
        reader.skip(member.getNextSnapshotOffset() * MAX_BATCH_SIZE);
        Buffer buffer = HeapBuffer.allocate(Math.min(MAX_BATCH_SIZE, reader.remaining()));
        reader.read(buffer);

        // Create the install request, indicating whether this is the last chunk of data based on the number
        // of bytes remaining in the buffer.
        Member leader = context.getLeader();
        request = InstallRequest.builder()
          .withTerm(context.getTerm())
          .withLeader(leader != null ? leader.id() : 0)
          .withIndex(member.getNextSnapshotIndex())
          .withOffset(member.getNextSnapshotOffset())
          .withData(buffer.flip())
          .withComplete(!reader.hasRemaining())
          .build();
      }
    }

    return request;
  }

  /**
   * Builds an append request for the given member.
   */
  protected abstract AppendRequest buildAppendRequest(MemberState member);

  /**
   * Gets the previous index.
   */
  protected long getPrevIndex(MemberState member) {
    return member.getNextIndex() - 1;
  }

  /**
   * Gets the previous entry.
   */
  protected Entry getPrevEntry(MemberState member, long prevIndex) {
    if (prevIndex > 0) {
      return context.getLog().get(prevIndex);
    }
    return null;
  }

  /**
   * Starts sending a request.
   */
  protected void startAppendRequest(MemberState member, AppendRequest request) {
    appending.add(member);
  }

  /**
   * Ends sending a request.
   */
  protected void endAppendRequest(MemberState member, AppendRequest request, Throwable error) {
    appending.remove(member);
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberState member, AppendRequest request) {
    startAppendRequest(member, request);

    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendAppendRequest(connection, member, request);
        } else {
          endAppendRequest(member, request, error);
          handleAppendError(member, request, error);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendAppendRequest(Connection connection, MemberState member, AppendRequest request) {
    connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
      endAppendRequest(member, request, error);
      context.checkThread();

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          handleAppendResponse(member, request, response);
        } else {
          handleAppendError(member, request, error);
        }
      }
    });
  }

  /**
   * Handles an append response.
   */
  protected abstract void handleAppendResponse(MemberState member, AppendRequest request, AppendResponse response);

  /**
   * Handles an append error.
   */
  protected abstract void handleAppendError(MemberState member, AppendRequest request, Throwable error);

  /**
   * Returns a boolean value indicating whether there are more entries to send.
   */
  protected boolean hasMoreEntries(MemberState member) {
    return member.getNextIndex() < context.getLog().lastIndex();
  }

  /**
   * Updates the match index when a response is received.
   */
  protected void updateMatchIndex(MemberState member, AppendResponse response) {
    // If the replica returned a valid match index then update the existing match index.
    member.setMatchIndex(response.logIndex());
  }

  /**
   * Updates the next index when the match index is updated.
   */
  protected void updateNextIndex(MemberState member) {
    // If the match index was set, update the next index to be greater than the match index if necessary.
    member.setNextIndex(Math.max(member.getNextIndex(), Math.max(member.getMatchIndex() + 1, 1)));
  }

  /**
   * Resets the match index when a response fails.
   */
  protected void resetMatchIndex(MemberState member, AppendResponse response) {
    member.setMatchIndex(response.logIndex());
    LOGGER.debug("{} - Reset match index for {} to {}", context.getCluster().getMember().serverAddress(), member, member.getMatchIndex());
  }

  /**
   * Resets the next index when a response fails.
   */
  protected void resetNextIndex(MemberState member) {
    if (member.getMatchIndex() != 0) {
      member.setNextIndex(member.getMatchIndex() + 1);
    } else {
      member.setNextIndex(context.getLog().firstIndex());
    }
    LOGGER.debug("{} - Reset next index for {} to {}", context.getCluster().getMember().serverAddress(), member, member.getNextIndex());
  }

  @Override
  public void close() {
    open = false;
  }

}
