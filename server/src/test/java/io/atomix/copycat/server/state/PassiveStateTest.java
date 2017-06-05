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

import io.atomix.copycat.protocol.CommandRequest;
import io.atomix.copycat.protocol.CommandResponse;
import io.atomix.copycat.protocol.QueryRequest;
import io.atomix.copycat.protocol.QueryResponse;
import io.atomix.copycat.protocol.Response.Status;
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.JoinRequest;
import io.atomix.copycat.server.protocol.JoinResponse;
import io.atomix.copycat.server.protocol.LeaveRequest;
import io.atomix.copycat.server.protocol.LeaveResponse;
import io.atomix.copycat.server.protocol.PollRequest;
import io.atomix.copycat.server.protocol.PollResponse;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.VoteResponse;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.TestEntry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Passive state tests.
 */
@Test
public class PassiveStateTest extends AbstractStateTest<PassiveState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new PassiveState(serverContext);
  }

  public void testDoAppendEntries() throws Throwable {
    // TODO
  }

  public void testDoCheckPreviousEntry() throws Throwable {
    // TODO
  }

  @SuppressWarnings("unchecked")
  public void testAppendUpdatesLeaderAndTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(members.get(1).hashCode())
        .withEntries(Collections.EMPTY_LIST)
        .withLogIndex(0)
        .withLogTerm(0)
        .withCommitIndex(0)
        .build();

      AppendResponse response = state.append(request).get();
      
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLeader().serverAddress(), members.get(1).serverAddress());
      threadAssertEquals(serverContext.getLastVotedFor(), 0);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.succeeded());
    });
  }

  @SuppressWarnings("unchecked")
  public void testAppendTermAndLeaderUpdated() throws Throwable {
    runOnServer(() -> {
      int leader = serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id();
      serverContext.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(leader)
        .withEntries(Collections.EMPTY_LIST)
        .withCommitIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(serverContext.getLeader().hashCode(), leader);
      assertEquals(response.term(), 2L);
    });
  }

  @SuppressWarnings("unchecked")
  public void testRejectAppendOnTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      append(2, 2);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withEntries(Collections.EMPTY_LIST)
        .withLogIndex(2)
        .withLogTerm(2)
        .withCommitIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.succeeded());
      assertEquals(response.term(), 2L);
      assertEquals(response.logIndex(), 2L);
    });
  }

  public void testAppendOnEmptyLog() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(0)
        .withLogTerm(0)
        .withCommitIndex(1)
        .addEntry(new Indexed<>(1, 1, new TestEntry(), 1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 1L);

      assertEquals(serverContext.getLogWriter().lastIndex(), 1L);
      assertNotNull(get(1));
    });
  }

  public void testAppendOnNonEmptyLog() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      append(1, 1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(0)
        .withLogTerm(0)
        .withCommitIndex(2)
        .addEntry(new Indexed<>(2, 1, new TestEntry(), 1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 2L);

      assertEquals(serverContext.getLogWriter().lastIndex(), 2L);
      assertNotNull(get(2));
    });
  }

  public void testCommandWithoutLeader() throws Throwable {
    runOnServer(() -> {
      CommandRequest request = CommandRequest.builder().withSession(1).withBytes(new byte[0]).build();
      CommandResponse response = state.command(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testJoinWithoutLeader() throws Throwable {
    runOnServer(() -> {
      JoinRequest request = JoinRequest.builder().withMember(members.get(0)).build();
      JoinResponse response = state.join(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testLeaveWithoutLeader() throws Throwable {
    runOnServer(() -> {
      LeaveRequest request = LeaveRequest.builder().withMember(members.get(0)).build();
      LeaveResponse response = state.leave(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testQueryWithoutLeader() throws Throwable {
    runOnServer(() -> {
      QueryRequest request = QueryRequest.builder().withSession(1).withBytes(new byte[0]).build();
      QueryResponse response = state.query(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testPoll() throws Throwable {
    runOnServer(() -> {
      PollRequest request = PollRequest.builder().withCandidate(1).withLogIndex(1).withLogTerm(1).withTerm(1).build();
      PollResponse response = state.poll(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testVote() throws Throwable {
    runOnServer(() -> {
      VoteRequest request = VoteRequest.builder().withCandidate(1).withLogIndex(1).withLogTerm(1).withTerm(1).build();
      VoteResponse response = state.vote(request).get();
      assertIllegalMemberStateError(response);
    });
  }

}
