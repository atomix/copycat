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
 * Abstract entries appender.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
abstract class AbstractAppender implements AutoCloseable {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerState context;
  private final Set<MemberState> appending = new HashSet<>();
  private final Set<MemberState> configured = new HashSet<>();
  private final Set<MemberState> configuring = new HashSet<>();
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

    LOGGER.debug("{} - Sent {} to {}", context.getMember().serverAddress(), request, member.getMember().serverAddress());
    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, request);
        } else {
          LOGGER.warn("{} - Failed to configure {}", context.getMember().serverAddress(), member.getMember().serverAddress());
          configuring.remove(member);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendConfigureRequest(Connection connection, MemberState member, ConfigureRequest request) {
    connection.<ConfigureRequest, ConfigureResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();
      configuring.remove(member);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getMember().serverAddress(), response, member.getMember().serverAddress());
          configured.add(member);
          appendEntries(member);
        } else {
          LOGGER.warn("{} - Failed to configure {}", context.getMember().serverAddress(), member.getMember().serverAddress());
        }
      }
    });
  }

  /**
   * Sends append entries requests to the given member.
   */
  protected void appendEntries(MemberState member) {
    if (!configured.contains(member) && open) {
      configure(member);
    } else if (!appending.contains(member) && open) {
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
      .withVersion(context.getVersion())
      .withMembers(context.buildMembers())
      .build();
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

    LOGGER.debug("{} - Sent {} to {}", context.getMember().serverAddress(), request, member.getMember().serverAddress());
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
          LOGGER.debug("{} - Received {} from {}", context.getMember().serverAddress(), response, member.getMember().serverAddress());
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
    LOGGER.debug("{} - Reset match index for {} to {}", context.getMember().serverAddress(), member, member.getMatchIndex());
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
    LOGGER.debug("{} - Reset next index for {} to {}", context.getMember().serverAddress(), member, member.getNextIndex());
  }

  @Override
  public void close() {
    open = false;
  }

}
