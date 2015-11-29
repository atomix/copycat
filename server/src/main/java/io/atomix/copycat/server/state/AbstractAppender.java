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

import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.cluster.MemberContext;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.ConfigureRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.ConfigureResponse;
import io.atomix.copycat.server.storage.entry.Entry;
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
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerStateController<RaftState> controller;
  private final Set<MemberContext> appending = new HashSet<>();
  private final Set<MemberContext> configuring = new HashSet<>();
  private boolean open = true;

  AbstractAppender(ServerStateController<RaftState> controller) {
    this.controller = controller;
  }

  /**
   * Sends a configuration to the given member.
   */
  protected void configure(MemberContext member) {
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
  protected void sendConfigureRequest(MemberContext member, ConfigureRequest request) {
    configuring.add(member);

    controller.context().getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      controller.context().checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, request);
        } else {
          LOGGER.warn("{} - Failed to configure {}", controller.context().getCluster().getMember().serverAddress(), member.getMember().serverAddress());
          configuring.remove(member);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendConfigureRequest(Connection connection, MemberContext member, ConfigureRequest request) {
    LOGGER.debug("{} - Sent {} to {}", controller.context().getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    connection.<ConfigureRequest, ConfigureResponse>send(request).whenComplete((response, error) -> {
      controller.context().checkThread();
      configuring.remove(member);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", controller.context().getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          member.setTerm(request.term()).setVersion(request.version());
          appendEntries(member);
        } else {
          LOGGER.warn("{} - Failed to configure {}", controller.context().getCluster().getMember().serverAddress(), member.getMember().serverAddress());
        }
      }
    });
  }

  /**
   * Sends append entries requests to the given member.
   */
  protected void appendEntries(MemberContext member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open)
      return;

    // If the member term is less than the current term or the member's configuration version is less
    // than the local configuration version, send a configuration update to the member.
    // Ensure that only one configuration attempt per member is attempted at any given time by storing the
    // member state in a set of configuring members.
    // Once the configuration is complete sendAppendRequest will be called recursively.
    if (member.getTerm() < controller.context().getTerm() || member.getVersion() < controller.context().getCluster().getVersion() && !configuring.contains(member)) {
      configure(member);
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
  protected ConfigureRequest buildConfigureRequest(MemberContext member) {
    Member leader = controller.context().getLeader();
    return ConfigureRequest.builder()
      .withTerm(controller.context().getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withVersion(controller.context().getCluster().getVersion())
      .withMembers(controller.context().getCluster().getMembers())
      .build();
  }

  /**
   * Builds an append request for the given member.
   */
  protected abstract AppendRequest buildAppendRequest(MemberContext member);

  /**
   * Gets the previous index.
   */
  protected long getPrevIndex(MemberContext member) {
    return member.getNextIndex() - 1;
  }

  /**
   * Gets the previous entry.
   */
  protected Entry getPrevEntry(MemberContext member, long prevIndex) {
    if (prevIndex > 0) {
      return controller.context().getLog().get(prevIndex);
    }
    return null;
  }

  /**
   * Starts sending a request.
   */
  protected void startAppendRequest(MemberContext member, AppendRequest request) {
    appending.add(member);
  }

  /**
   * Ends sending a request.
   */
  protected void endAppendRequest(MemberContext member, AppendRequest request, Throwable error) {
    appending.remove(member);
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberContext member, AppendRequest request) {
    startAppendRequest(member, request);

    LOGGER.debug("{} - Sent {} to {}", controller.context().getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    controller.context().getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      controller.context().checkThread();

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
  protected void sendAppendRequest(Connection connection, MemberContext member, AppendRequest request) {
    connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
      endAppendRequest(member, request, error);
      controller.context().checkThread();

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", controller.context().getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
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
  protected abstract void handleAppendResponse(MemberContext member, AppendRequest request, AppendResponse response);

  /**
   * Handles an append error.
   */
  protected abstract void handleAppendError(MemberContext member, AppendRequest request, Throwable error);

  /**
   * Returns a boolean value indicating whether there are more entries to send.
   */
  protected boolean hasMoreEntries(MemberContext member) {
    return member.getNextIndex() < controller.context().getLog().lastIndex();
  }

  /**
   * Updates the match index when a response is received.
   */
  protected void updateMatchIndex(MemberContext member, AppendResponse response) {
    // If the replica returned a valid match index then update the existing match index.
    member.setMatchIndex(response.logIndex());
  }

  /**
   * Updates the next index when the match index is updated.
   */
  protected void updateNextIndex(MemberContext member) {
    // If the match index was set, update the next index to be greater than the match index if necessary.
    member.setNextIndex(Math.max(member.getNextIndex(), Math.max(member.getMatchIndex() + 1, 1)));
  }

  /**
   * Resets the match index when a response fails.
   */
  protected void resetMatchIndex(MemberContext member, AppendResponse response) {
    member.setMatchIndex(response.logIndex());
    LOGGER.debug("{} - Reset match index for {} to {}", controller.context().getCluster().getMember().serverAddress(), member, member.getMatchIndex());
  }

  /**
   * Resets the next index when a response fails.
   */
  protected void resetNextIndex(MemberContext member) {
    if (member.getMatchIndex() != 0) {
      member.setNextIndex(member.getMatchIndex() + 1);
    } else {
      member.setNextIndex(controller.context().getLog().firstIndex());
    }
    LOGGER.debug("{} - Reset next index for {} to {}", controller.context().getCluster().getMember().serverAddress(), member, member.getNextIndex());
  }

  @Override
  public void close() {
    open = false;
  }

}
