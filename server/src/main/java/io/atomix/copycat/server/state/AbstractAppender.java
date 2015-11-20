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
import io.atomix.copycat.server.response.AppendResponse;
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
  private boolean open = true;

  AbstractAppender(ServerState context) {
    this.context = context;
  }

  /**
   * Sends append entries requests to the given member.
   */
  protected void appendEntries(MemberState member) {
    if (!appending.contains(member) && open) {
      AppendRequest request = buildRequest(member);
      if (request != null) {
        sendRequest(member, request);
      }
    }
  }

  /**
   * Builds an append request for the given member.
   */
  protected abstract AppendRequest buildRequest(MemberState member);

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
  protected void startRequest(MemberState member, AppendRequest request) {
    appending.add(member);
  }

  /**
   * Ends sending a request.
   */
  protected void endRequest(MemberState member, AppendRequest request, Throwable error) {
    appending.remove(member);
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendRequest(MemberState member, AppendRequest request) {
    startRequest(member, request);

    LOGGER.debug("{} - Sent {} to {}", context.getMember().serverAddress(), request, member.getMember().serverAddress());
    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendRequest(connection, member, request);
        } else {
          endRequest(member, request, error);
          handleError(member, request, error);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendRequest(Connection connection, MemberState member, AppendRequest request) {
    connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
      endRequest(member, request, error);
      context.checkThread();

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getMember().serverAddress(), response, member.getMember().serverAddress());
          handleResponse(member, request, response);
        } else {
          handleError(member, request, error);
        }
      }
    });
  }

  /**
   * Handles an append response.
   */
  protected abstract void handleResponse(MemberState member, AppendRequest request, AppendResponse response);

  /**
   * Handles an append error.
   */
  protected abstract void handleError(MemberState member, AppendRequest request, Throwable error);

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
