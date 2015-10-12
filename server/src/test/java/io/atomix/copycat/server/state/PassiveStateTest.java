package io.atomix.copycat.server.state;

import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.request.CommandRequest;
import io.atomix.copycat.client.request.ConnectRequest;
import io.atomix.copycat.client.request.KeepAliveRequest;
import io.atomix.copycat.client.request.PublishRequest;
import io.atomix.copycat.client.request.QueryRequest;
import io.atomix.copycat.client.request.RegisterRequest;
import io.atomix.copycat.client.request.UnregisterRequest;
import io.atomix.copycat.client.response.AbstractResponse;
import io.atomix.copycat.client.response.CommandResponse;
import io.atomix.copycat.client.response.ConnectResponse;
import io.atomix.copycat.client.response.KeepAliveResponse;
import io.atomix.copycat.client.response.PublishResponse;
import io.atomix.copycat.client.response.QueryResponse;
import io.atomix.copycat.client.response.RegisterResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.client.response.Response.Status;
import io.atomix.copycat.client.response.UnregisterResponse;
import io.atomix.copycat.server.TestStateMachine.TestCommand;
import io.atomix.copycat.server.TestStateMachine.TestQuery;
import io.atomix.copycat.server.request.AcceptRequest;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.JoinRequest;
import io.atomix.copycat.server.request.LeaveRequest;
import io.atomix.copycat.server.request.PollRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AcceptResponse;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.JoinResponse;
import io.atomix.copycat.server.response.LeaveResponse;
import io.atomix.copycat.server.response.PollResponse;
import io.atomix.copycat.server.response.VoteResponse;

@Test
public class PassiveStateTest extends AbstractStateTest {
  PassiveState state;

  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new PassiveState(serverState);
  }

  private void assertNoLeaderError(AbstractResponse<?> response) {
    threadAssertEquals(response.status(), Response.Status.ERROR);
    threadAssertEquals(response.error(), RaftError.Type.NO_LEADER_ERROR);
  }

  private void assertIllegalMemberStateError(AbstractResponse<?> response) {
    threadAssertEquals(response.status(), Response.Status.ERROR);
    threadAssertEquals(response.error(), RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR);
  }

  public void testAccept() throws Throwable {
    runOnServer(() -> {
      AcceptRequest request = AcceptRequest.builder().withAddress(members.get(0)).withSession(1).build();
      AcceptResponse response = state.accept(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testKeepAlive() throws Throwable {
    runOnServer(() -> {
      KeepAliveRequest request = KeepAliveRequest.builder()
          .withCommandSequence(1)
          .withEventVersion(1)
          .withSession(1)
          .build();
      KeepAliveResponse response = state.keepAlive(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testApplyCommits() throws Throwable {
    runOnServer(() -> {
      state.applyCommits(3);

      // TODO more assertions
      threadAssertEquals(serverState.getCommitIndex(), 3L);
    });
  }

  public void testDoAppendEntries() throws Throwable {
    // TODO
  }

  public void testDoCheckPreviousEntry() throws Throwable {
    // TODO
  }

  public void testCommandWithoutLeader() throws Throwable {
    runOnServer(() -> {
      CommandRequest request = CommandRequest.builder().withSession(1).withCommand(new TestCommand("test")).build();
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
      QueryRequest request = QueryRequest.builder().withSession(1).withQuery(new TestQuery()).build();
      QueryResponse response = state.query(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testRegister() throws Throwable {
    runOnServer(() -> {
      RegisterRequest request = RegisterRequest.builder().withClient(UUID.randomUUID()).build();
      RegisterResponse response = state.register(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testUnregister() throws Throwable {
    runOnServer(() -> {
      UnregisterRequest request = UnregisterRequest.builder().withSession(1).build();
      UnregisterResponse response = state.unregister(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testConnect() throws Throwable {
    runOnServer(() -> {
      ConnectRequest request = ConnectRequest.builder().withSession(1).build();
      ConnectResponse response = state.connect(request, null).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testPoll() throws Throwable {
    runOnServer(() -> {
      PollRequest request = PollRequest.builder().withCandidate(1).withLogIndex(1).withLogTerm(1).withTerm(1).build();
      PollResponse response = state.poll(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testPublish() throws Throwable {
    runOnServer(() -> {
      PublishRequest request = PublishRequest.builder().withSession(1).withEventVersion(1).build();
      PublishResponse response = state.publish(request).get();
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

  public void testForward() throws Throwable {
    // TODO
  }

  public void testAppendUpdatesLeaderAndTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);

      AppendRequest request = AppendRequest.builder()
          .withTerm(2)
          .withLeader(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .withCommitIndex(0)
          .withGlobalIndex(0)
          .build();

      AppendResponse response = state.append(request).get();
      threadAssertEquals(serverState.getTerm(), 2L);
      threadAssertEquals(serverState.getLeader(), members.get(1));
      threadAssertEquals(serverState.getLastVotedFor(), 0);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.succeeded());
    });
  }

  public void testAppendRejectedWhenRequestTermIsOld() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(3);
      AppendRequest request = AppendRequest.builder().withTerm(2).build();

      AppendResponse response = state.append(request).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverState.getTerm(), 3L);
      threadAssertEquals(response.term(), 3L);
      threadAssertFalse(response.succeeded());
    });
  }

  public void testHandleAppend() throws Throwable {
    // TODO
  }

  public void testHandleAppendRejectedWhenRequestTermIsOld() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(3);
      AppendRequest request = AppendRequest.builder().withTerm(2).build();

      AppendResponse response = state.handleAppend(request);

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverState.getTerm(), 3L);
      threadAssertEquals(response.term(), 3L);
      threadAssertFalse(response.succeeded());
    });
  }
}
