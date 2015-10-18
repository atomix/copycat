package io.atomix.copycat.server.state;

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.atomix.copycat.client.response.Response.Status;
import io.atomix.copycat.server.RaftServer.State;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.response.AppendResponse;

@Test
public class ActiveStateTest extends AbstractStateTest<ActiveState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new ActiveState(serverState) {
    };
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

      assertEquals(serverState.getTerm(), 2L);
      assertEquals(serverState.getLeader(), members.get(1));
      assertEquals(serverState.getLastVotedFor(), 0);
      assertEquals(response.term(), 2L);
      assertTrue(response.succeeded());
    });
  }

  public void testAppendTermUpdatedAndTransitionedToFollower() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      AppendRequest request = AppendRequest.builder().withTerm(2).build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
      assertEquals(serverState.getState(), State.FOLLOWER);
    });
  }

  public void testHandlePoll() throws Throwable {
    // TODO
  }

  public void testHandleVote() throws Throwable {
    // TODO
  }

  public void testIsLogUpToDate() throws Throwable {
    // TODO
  }

  public void testQuery() throws Throwable {
    // TODO
  }
}
