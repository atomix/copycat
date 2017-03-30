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

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.*;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
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
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final ServerContext context;
  protected boolean open = true;

  AbstractAppender(ServerContext context) {
    this.context = Assert.notNull(context, "context");
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
  protected AppendRequest buildAppendRequest(MemberState member, long lastIndex) {
    // If the log is empty then send an empty commit.
    // If the next index hasn't yet been set then we send an empty commit first.
    // If the next index is greater than the last index then send an empty commit.
    // If the member failed to respond to recent communication send an empty commit. This
    // helps avoid doing expensive work until we can ascertain the member is back up.
    if (context.getLog().isEmpty() || member.getNextIndex() > lastIndex || member.getFailureCount() > 0) {
      return buildAppendEmptyRequest(member);
    } else {
      return buildAppendEntriesRequest(member, lastIndex);
    }
  }

  /**
   * Builds an empty AppendEntries request.
   * <p>
   * Empty append requests are used as heartbeats to followers.
   */
  @SuppressWarnings("unchecked")
  protected AppendRequest buildAppendEmptyRequest(MemberState member) {
    Entry prevEntry = getPrevEntry(member);

    ServerMember leader = context.getLeader();
    return AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevEntry != null ? prevEntry.getIndex() : 0)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withEntries(Collections.EMPTY_LIST)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex())
      .build();
  }

  /**
   * Builds a populated AppendEntries request.
   */
  @SuppressWarnings("unchecked")
  protected AppendRequest buildAppendEntriesRequest(MemberState member, long lastIndex) {
    Entry prevEntry = getPrevEntry(member);

    ServerMember leader = context.getLeader();
    AppendRequest.Builder builder = AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevEntry != null ? prevEntry.getIndex() : 0)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex());

    // Calculate the starting index of the list of entries.
    final long index = prevEntry != null ? prevEntry.getIndex() + 1 : context.getLog().firstIndex();

    // Build a list of entries to send to the member.
    List<Entry> entries = new ArrayList<>((int) Math.min(8, lastIndex - index + 1));

    // Build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
    // be null if they've been compacted and the member to which we're sending entries is just
    // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
    // counted towards the size of the batch.
    // If there exists an entry in the log with size >= MAX_BATCH_SIZE the logic ensures that
    // entry will be sent in a batch of size one
    int size = 0;

    // Iterate through remaining entries in the log up to the last index.
    for (long i = index; i <= lastIndex; i++) {
      // Get the entry from the log and append it if it's not null. Entries in the log can be null
      // if they've been cleaned or compacted from the log. Each entry sent in the append request
      // has a unique index to handle gaps in the log.
      Entry entry = context.getLog().get(i);
      if (entry != null) {
        if (!entries.isEmpty() && size + entry.size() > MAX_BATCH_SIZE) {
          break;
        }
        size += entry.size();
        entries.add(entry);
      }
    }

    // Release the previous entry back to the entry pool.
    if (prevEntry != null) {
      prevEntry.release();
    }

    // Add the entries to the request builder and build the request.
    return builder.withEntries(entries).build();
  }

  /**
   * Gets the previous entry.
   */
  @SuppressWarnings("unused")
  protected Entry getPrevEntry(MemberState member) {
    long prevIndex = Math.min(member.getNextIndex() - 1, context.getLog().lastIndex());
    while (prevIndex > 0) {
      Entry entry = context.getLog().get(prevIndex);
      if (entry != null) {
        return entry;
      }
      prevIndex--;
    }
    return null;
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberState member, AppendRequest request) {
    // Start the append to the member.
    member.startAppend();

    context.getConnections().getConnection(member.getMember().address()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendAppendRequest(connection, member, request);
        } else {
          // Complete the append to the member.
          member.completeAppend();

          // Trigger reactions to the request failure.
          handleAppendRequestFailure(member, request, error);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendAppendRequest(Connection connection, MemberState member, AppendRequest request) {
    long timestamp = System.nanoTime();

    logger.trace("{} - Sending {} to {}", context.getCluster().member().address(), request, member.getMember().address());
    connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the append to the member.
      if (!request.entries().isEmpty()) {
        member.completeAppend(System.nanoTime() - timestamp);
      } else {
        member.completeAppend();
      }

      if (open) {
        if (error == null) {
          logger.trace("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().address());
          handleAppendResponse(member, request, response);
        } else {
          handleAppendResponseFailure(member, request, error);
        }
      }
    });

    updateNextIndex(member, request);
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
    if (response.status() == Response.Status.OK) {
      handleAppendResponseOk(member, request, response);
    } else {
      handleAppendResponseError(member, request, response);
    }
  }

  /**
   * Handles a {@link Response.Status#OK} response.
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
   * Handles a {@link Response.Status#ERROR} response.
   */
  protected void handleAppendResponseError(MemberState member, AppendRequest request, AppendResponse response) {
    // If any other error occurred, increment the failure count for the member. Log the first three failures,
    // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
    // when attempting to send entries to down followers.
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      logger.warn("{} - AppendRequest to {} failed: {}", context.getCluster().member().address(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
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
      logger.warn("{} - AppendRequest to {} failed: {}", context.getCluster().member().address(), member.getMember().address(), error.getMessage());
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
   * Updates the next index when the match index is updated.
   */
  protected void updateNextIndex(MemberState member, AppendRequest request) {
    // If the match index was set, update the next index to be greater than the match index if necessary.
    if (!request.entries().isEmpty()) {
      member.setNextIndex(request.entries().get(request.entries().size()-1).getIndex()+1);
    }
  }

  /**
   * Resets the match index when a response fails.
   */
  protected void resetMatchIndex(MemberState member, AppendResponse response) {
    member.setMatchIndex(response.logIndex());
    logger.trace("{} - Reset match index for {} to {}", context.getCluster().member().address(), member, member.getMatchIndex());
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
    logger.trace("{} - Reset next index for {} to {}", context.getCluster().member().address(), member, member.getNextIndex());
  }

  /**
   * Builds a configure request for the given member.
   */
  protected ConfigureRequest buildConfigureRequest(MemberState member) {
    ServerMember leader = context.getLeader();
    return ConfigureRequest.builder()
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
  protected void sendConfigureRequest(MemberState member, ConfigureRequest request) {
    logger.debug("{} - Configuring {}", context.getCluster().member().address(), member.getMember().address());

    // Start the configure to the member.
    member.startConfigure();

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, request);
        } else {
          // Complete the configure to the member.
          member.completeConfigure();

          // Trigger reactions to the request failure.
          handleConfigureRequestFailure(member, request, error);
        }
      }
    });
  }

  /**
   * Sends a configuration message.
   */
  protected void sendConfigureRequest(Connection connection, MemberState member, ConfigureRequest request) {
    logger.trace("{} - Sending {} to {}", context.getCluster().member().address(), request, member.getMember().serverAddress());
    connection.<ConfigureRequest, ConfigureResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the configure to the member.
      member.completeConfigure();

      if (open) {
        if (error == null) {
          logger.trace("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().serverAddress());
          handleConfigureResponse(member, request, response);
        } else {
          logger.warn("{} - Failed to configure {}", context.getCluster().member().address(), member.getMember().serverAddress());
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
    if (response.status() == Response.Status.OK) {
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
        byte[] data = new byte[Math.min(MAX_BATCH_SIZE, (int) reader.remaining())];
        reader.read(data);

        // Create the install request, indicating whether this is the last chunk of data based on the number
        // of bytes remaining in the buffer.
        ServerMember leader = context.getLeader();
        request = InstallRequest.builder()
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
  protected void sendInstallRequest(MemberState member, InstallRequest request) {
    // Start the install to the member.
    member.startInstall();

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendInstallRequest(connection, member, request);
        } else {
          // Complete the install to the member.
          member.completeInstall();

          // Trigger reactions to the install request failure.
          handleInstallRequestFailure(member, request, error);
        }
      }
    });
  }

  /**
   * Sends a snapshot message.
   */
  protected void sendInstallRequest(Connection connection, MemberState member, InstallRequest request) {
    logger.trace("{} - Sending {} to {}", context.getCluster().member().address(), request, member.getMember().serverAddress());
    connection.<InstallRequest, InstallResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Complete the install to the member.
      member.completeInstall();

      if (open) {
        if (error == null) {
          logger.trace("{} - Received {} from {}", context.getCluster().member().address(), response, member.getMember().serverAddress());
          handleInstallResponse(member, request, response);
        } else {
          logger.warn("{} - Failed to install {}", context.getCluster().member().address(), member.getMember().serverAddress());

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
    if (response.status() == Response.Status.OK) {
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
    logger.warn("{} - Failed to install {}", context.getCluster().member().address(), member.getMember().serverAddress());
    member.setNextSnapshotIndex(0).setNextSnapshotOffset(0);
  }

  @Override
  public void close() {
    open = false;
  }

}
