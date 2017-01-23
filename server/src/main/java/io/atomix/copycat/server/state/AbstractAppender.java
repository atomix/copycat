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
package io.atomix.copycat.server.state;

import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.protocol.request.ConfigureRequest;
import io.atomix.copycat.server.protocol.request.InstallRequest;
import io.atomix.copycat.server.protocol.response.AppendResponse;
import io.atomix.copycat.server.protocol.response.ConfigureResponse;
import io.atomix.copycat.server.protocol.response.InstallResponse;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.LogReader;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract appender.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
abstract class AbstractAppender implements AutoCloseable {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerContext context;
  protected boolean open = true;

  protected AbstractAppender(ServerContext context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Logs a request by wrapping the request factory in a logging factory.
   */
  protected <T extends ProtocolRequest.Builder<T, U>, U extends ProtocolRequest> ProtocolRequestFactory<T, U> logRequest(ProtocolRequestFactory<T, U> factory, MemberState member) {
    return builder -> {
      U request = factory.build(builder);
      LOGGER.debug("{} - Sent {} to {}", context.getCluster().member().address(), request, member.getMember().serverAddress());
      return request;
    };
  }

  /**
   * Logs a response received from a member.
   */
  protected void logResponse(ProtocolResponse response, MemberState member) {
    LOGGER.debug("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().serverAddress());
  }

  /**
   * Sends an AppendRequest to the given member.
   *
   * @param member The member to which to send the append request.
   */
  protected abstract void appendEntries(MemberState member);

  /**
   * Builds an append request.
   *
   * @param member The member to which to send the request.
   * @return The append request.
   */
  protected AppendRequest buildAppendRequest(MemberState member, AppendRequest.Builder builder, long lastIndex) {
    final LogReader reader = member.getLogReader();
    try {
      // Lock the entry reader.
      reader.lock().lock();

      // If the log is empty then send an empty commit.
      // If the next index hasn't yet been set then we send an empty commit first.
      // If the next index is greater than the last index then send an empty commit.
      // If the member failed to respond to recent communication send an empty commit. This
      // helps avoid doing expensive work until we can ascertain the member is back up.
      if (!reader.hasNext() || member.getFailureCount() > 0) {
        return buildAppendEmptyRequest(member, builder);
      } else {
        return buildAppendEntriesRequest(member, builder, lastIndex);
      }
    } finally {
      // Unlock the entry reader.
      reader.lock().unlock();
    }
  }

  /**
   * Builds an empty AppendEntries request.
   * <p>
   * Empty append requests are used as heartbeats to followers.
   */
  @SuppressWarnings("unchecked")
  protected AppendRequest buildAppendEmptyRequest(MemberState member, AppendRequest.Builder builder) {
    final LogReader reader = member.getLogReader();

    // Acquire the reader lock.
    reader.lock().lock();

    // Read the previous entry from the reader.
    Indexed<? extends Entry<?>> prevEntry = reader.entry();

    // Release the reader lock.
    reader.lock().unlock();

    ServerMember leader = context.getLeader();
    return builder
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevEntry != null ? prevEntry.index() : 0)
      .withLogTerm(prevEntry != null ? prevEntry.term() : 0)
      .withEntries(Collections.EMPTY_LIST)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex())
      .build();
  }

  /**
   * Builds a populated AppendEntries request.
   */
  @SuppressWarnings("unchecked")
  protected AppendRequest buildAppendEntriesRequest(MemberState member, AppendRequest.Builder builder, long lastIndex) {
    final LogReader reader = member.getLogReader();

    // Acquire the reader lock.
    reader.lock().lock();

    final Indexed<? extends Entry<?>> prevEntry = reader.entry();

    final ServerMember leader = context.getLeader();
    builder
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevEntry != null ? prevEntry.index() : 0)
      .withLogTerm(prevEntry != null ? prevEntry.term() : 0)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex());

    // Build a list of entries to send to the member.
    final List<Indexed<? extends Entry<?>>> entries = new ArrayList<>();

    // Build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
    // be null if they've been compacted and the member to which we're sending entries is just
    // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
    // counted towards the size of the batch.
    // If there exists an entry in the log with size >= MAX_BATCH_SIZE the logic ensures that
    // entry will be sent in a batch of size one
    int size = 0;

    // Iterate through the log until the last index or the end of the log is reached.
    while (reader.hasNext()) {
      Indexed<? extends Entry<?>> entry = reader.next();
      entries.add(entry);
      size += entry.size();
      if (size >= MAX_BATCH_SIZE) {
        break;
      }
    }

    // Release the reader lock.
    reader.lock().unlock();

    // Add the entries to the request builder and build the request.
    return builder.withEntries(entries).build();
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberState member, ProtocolRequestFactory<AppendRequest.Builder, AppendRequest> factory) {
    // Start the append to the member.
    member.startAppend();

    AppendRequest prototype = factory.build(new AppendRequest.Builder());

    LOGGER.debug("{} - Sent {} to {}", context.getCluster().member().address(), prototype, member.getMember().address());
    context.getConnections().getConnection(member.getMember().address()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendAppendRequest(connection, member, prototype);
        } else {
          // Complete the append to the member.
          member.completeAppend();

          // Trigger reactions to the request failure.
          handleAppendRequestFailure(member, prototype, error);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendAppendRequest(RaftProtocolClientConnection connection, MemberState member, AppendRequest request) {
    long timestamp = System.nanoTime();
    connection.append(logRequest(builder -> builder.copy(request), member)).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the append to the member.
      if (!request.entries().isEmpty()) {
        member.completeAppend(System.nanoTime() - timestamp);
      } else {
        member.completeAppend();
      }

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().address());
          handleAppendResponse(member, request, response);
        } else {
          handleAppendResponseFailure(member, request, error);
        }
      }
    });

    if (!request.entries().isEmpty() && hasMoreEntries(member)) {
      appendEntries(member);
    }
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendRequestFailure(MemberState member, AppendRequest request, Throwable error) {
    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendResponseFailure(MemberState member, AppendRequest request, Throwable error) {
    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles an append response.
   */
  protected void handleAppendResponse(MemberState member, AppendRequest request, AppendResponse response) {
    if (response.status() == ProtocolResponse.Status.OK) {
      handleAppendResponseOk(member, request, response);
    } else {
      handleAppendResponseError(member, request, response);
    }
  }

  /**
   * Handles a {@link ProtocolResponse.Status#OK} response.
   */
  protected void handleAppendResponseOk(MemberState member, AppendRequest request, AppendResponse response) {
    // Reset the member failure count and update the member's availability status if necessary.
    succeedAttempt(member);

    // If replication succeeded then trigger commit futures.
    if (response.succeeded()) {
      updateMatchIndex(member, response);

      // If there are more entries to send then attempt to send another commit.
      if (request.logIndex() != response.logIndex() && hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
    // If we've received a greater term, update the term and transition back to follower.
    else if (response.term() > context.getTerm()) {
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    }
    // If the response failed, the follower should have provided the correct last index in their log. This helps
    // us converge on the matchIndex faster than by simply decrementing nextIndex one index at a time.
    else {
      resetMatchIndex(member, response);
      resetNextIndex(member);

      // If there are more entries to send then attempt to send another commit.
      if (response.logIndex() != request.logIndex() && hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
  }

  /**
   * Handles a {@link ProtocolResponse.Status#ERROR} response.
   */
  protected void handleAppendResponseError(MemberState member, AppendRequest request, AppendResponse response) {
    // If any other error occurred, increment the failure count for the member. Log the first three failures,
    // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
    // when attempting to send entries to down followers.
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      LOGGER.warn("{} - AppendRequest to {} failed. Reason: [{}]", context.getCluster().member().address(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
    }
  }

  /**
   * Succeeds an attempt to contact a member.
   */
  protected void succeedAttempt(MemberState member) {
    // Reset the member failure count and time.
    member.resetFailureCount();
  }

  /**
   * Fails an attempt to contact a member.
   */
  protected void failAttempt(MemberState member, Throwable error) {
    // Reset the connection to the given member to ensure failed connections are reconstructed upon retries.
    context.getConnections().resetConnection(member.getMember().serverAddress());

    // If any append error occurred, increment the failure count for the member. Log the first three failures,
    // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
    // when attempting to send entries to down followers.
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      LOGGER.warn("{} - AppendRequest to {} failed. Reason: {}", context.getCluster().member().address(), member.getMember().address(), error.getMessage());
    }
  }

  /**
   * Returns a boolean value indicating whether there are more entries to send.
   */
  protected abstract boolean hasMoreEntries(MemberState member);

  /**
   * Updates the match index when a response is received.
   */
  protected void updateMatchIndex(MemberState member, AppendResponse response) {
    // If the replica returned a valid match index then update the existing match index.
    member.setMatchIndex(response.logIndex());
  }

  /**
   * Resets the match index when a response fails.
   */
  protected void resetMatchIndex(MemberState member, AppendResponse response) {
    member.setMatchIndex(response.logIndex());
    LOGGER.debug("{} - Reset match index for {} to {}", context.getCluster().member().address(), member, member.getMatchIndex());
  }

  /**
   * Resets the next index when a response fails.
   */
  protected void resetNextIndex(MemberState member) {
    final LogReader reader = member.getLogReader();
    try {
      reader.lock().lock();

      if (member.getMatchIndex() != 0) {
        reader.reset(member.getMatchIndex());
      } else {
        reader.reset();
      }
      LOGGER.debug("{} - Reset next index for {} to {} + 1", context.getCluster().member().address(), member, member.getMatchIndex());
    } finally {
      reader.lock().unlock();
    }
  }

  /**
   * Builds a configure request for the given member.
   */
  protected ConfigureRequest buildConfigureRequest(MemberState member, ConfigureRequest.Builder builder) {
    ServerMember leader = context.getLeader();
    return builder
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withIndex(context.getClusterState().getConfiguration().index())
      .withTime(context.getClusterState().getConfiguration().time())
      .withMembers(context.getClusterState().getConfiguration().members())
      .build();
  }

  /**
   * Connects to the member and sends a configure request.
   */
  protected void sendConfigureRequest(MemberState member, ProtocolRequestFactory<ConfigureRequest.Builder, ConfigureRequest> factory) {
    // Start the configure to the member.
    member.startConfigure();

    ConfigureRequest prototype = factory.build(new ConfigureRequest.Builder());

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, prototype);
        } else {
          // Complete the configure to the member.
          member.completeConfigure();

          // Trigger reactions to the request failure.
          handleConfigureRequestFailure(member, prototype, error);
        }
      }
    });
  }

  /**
   * Sends a configuration message.
   */
  protected void sendConfigureRequest(RaftProtocolClientConnection connection, MemberState member, ConfigureRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().member().address(), request, member.getMember().serverAddress());
    connection.configure(logRequest(builder -> builder.copy(request), member)).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the configure to the member.
      member.completeConfigure();

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().serverAddress());
          handleConfigureResponse(member, request, response);
        } else {
          LOGGER.warn("{} - Failed to configure {}", context.getCluster().member().address(), member.getMember().serverAddress());
          handleConfigureResponseFailure(member, request, error);
        }
      }
    });
  }

  /**
   * Handles a configure failure.
   */
  protected void handleConfigureRequestFailure(MemberState member, ConfigureRequest request, Throwable error) {
    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles a configure failure.
   */
  protected void handleConfigureResponseFailure(MemberState member, ConfigureRequest request, Throwable error) {
    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles a configuration response.
   */
  protected void handleConfigureResponse(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    if (response.status() == ProtocolResponse.Status.OK) {
      handleConfigureResponseOk(member, request, response);
    } else {
      handleConfigureResponseError(member, request, response);
    }
  }

  /**
   * Handles an OK configuration response.
   */
  @SuppressWarnings("unused")
  protected void handleConfigureResponseOk(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    // Reset the member failure count and update the member's status if necessary.
    succeedAttempt(member);

    // Update the member's current configuration term and index according to the installed configuration.
    member.setConfigTerm(request.term()).setConfigIndex(request.index());

    // Recursively append entries to the member.
    appendEntries(member);
  }

  /**
   * Handles an ERROR configuration response.
   */
  @SuppressWarnings("unused")
  protected void handleConfigureResponseError(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    // In the event of a configure response error, simply do nothing and await the next heartbeat.
    // This prevents infinite loops when cluster configurations fail.
  }

  /**
   * Builds an install request for the given member.
   */
  protected InstallRequest buildInstallRequest(MemberState member, InstallRequest.Builder requestBuilder) {
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
        byte[] data = new byte[Math.min(MAX_BATCH_SIZE, (int) reader.remaining())];
        reader.read(data);

        // Create the install request, indicating whether this is the last chunk of data based on the number
        // of bytes remaining in the buffer.
        ServerMember leader = context.getLeader();
        request = requestBuilder
          .withTerm(context.getTerm())
          .withLeader(leader != null ? leader.id() : 0)
          .withIndex(member.getNextSnapshotIndex())
          .withOffset(member.getNextSnapshotOffset())
          .withData(data)
          .withComplete(!reader.hasRemaining())
          .build();
      }
    }

    return request;
  }

  /**
   * Connects to the member and sends a snapshot request.
   */
  protected void sendInstallRequest(MemberState member, ProtocolRequestFactory<InstallRequest.Builder, InstallRequest> factory) {
    // Start the install to the member.
    member.startInstall();

    InstallRequest prototype = factory.build(new InstallRequest.Builder());
    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendInstallRequest(connection, member, prototype);
        } else {
          // Complete the install to the member.
          member.completeInstall();

          // Trigger reactions to the install request failure.
          handleInstallRequestFailure(member, prototype, error);
        }
      }
    });
  }

  /**
   * Sends a snapshot message.
   */
  protected void sendInstallRequest(RaftProtocolClientConnection connection, MemberState member, InstallRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().member().address(), request, member.getMember().serverAddress());
    connection.install(logRequest(builder -> builder.copy(request), member)).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the install to the member.
      member.completeInstall();

      if (open) {
        if (error == null) {
          logResponse(response, member);
          handleInstallResponse(member, request, response);
        } else {
          LOGGER.warn("{} - Failed to install {}", context.getCluster().member().address(), member.getMember().serverAddress());

          // Trigger reactions to the install response failure.
          handleInstallResponseFailure(member, request, error);
        }
      }
    });
  }

  /**
   * Handles an install request failure.
   */
  protected void handleInstallRequestFailure(MemberState member, InstallRequest request, Throwable error) {
    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles an install response failure.
   */
  protected void handleInstallResponseFailure(MemberState member, InstallRequest request, Throwable error) {
    // Reset the member's snapshot index and offset to resend the snapshot from the start
    // once a connection to the member is re-established.
    member.setNextSnapshotIndex(0).setNextSnapshotOffset(0);

    // Log the failed attempt to contact the member.
    failAttempt(member, error);
  }

  /**
   * Handles an install response.
   */
  protected void handleInstallResponse(MemberState member, InstallRequest request, InstallResponse response) {
    if (response.status() == ProtocolResponse.Status.OK) {
      handleInstallResponseOk(member, request, response);
    } else {
      handleInstallResponseError(member, request, response);
    }
  }

  /**
   * Handles an OK install response.
   */
  @SuppressWarnings("unused")
  protected void handleInstallResponseOk(MemberState member, InstallRequest request, InstallResponse response) {
    // Reset the member failure count and update the member's status if necessary.
    succeedAttempt(member);

    // If the install request was completed successfully, set the member's snapshotIndex and reset
    // the next snapshot index/offset.
    if (request.complete()) {
      member.setSnapshotIndex(request.index())
        .setNextSnapshotIndex(0)
        .setNextSnapshotOffset(0);
    }
    // If more install requests remain, increment the member's snapshot offset.
    else {
      member.setNextSnapshotOffset(request.offset() + 1);
    }

    // Recursively append entries to the member.
    appendEntries(member);
  }

  /**
   * Handles an ERROR install response.
   */
  @SuppressWarnings("unused")
  protected void handleInstallResponseError(MemberState member, InstallRequest request, InstallResponse response) {
    LOGGER.warn("{} - Failed to install {}", context.getCluster().member().address(), member.getMember().serverAddress());
    member.setNextSnapshotIndex(0).setNextSnapshotOffset(0);
  }

  @Override
  public void close() {
    open = false;
  }

}
