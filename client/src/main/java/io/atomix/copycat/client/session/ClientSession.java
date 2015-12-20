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
 * limitations under the License.
 */
package io.atomix.copycat.client.session;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.SubmissionStrategy;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.error.UnknownSessionException;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Handles submission of client {@link Command commands} and {@link Query queries}.
 * <p>
 * The client session handles all aspects of managing a single session for a client. When the session is opened,
 * it attempts to find a server in the provided servers list and {@link RegisterRequest register} a new session. Once
 * the session has been registered, it begins sending periodic {@link KeepAliveRequest keep-alive requests} and begins
 * submitting {@link Command commands} and {@link Query queries} to the cluster.
 * <p>
 * Sessions can communicate with any server in the cluster, but they'll attempt to find the best server.
 * In the event that the session becomes disconnected from the cluster or otherwise can't successfully commit a
 * keep-alive request, the session will attempt to find a better server with which to communicate. If the session
 * fails to communicate with the cluster for more than its session timeout, it will assume it expired and the session
 * will be closed.
 * <p>
 * The session is responsible for coordinating with the cluster to ensure consistency constraints are met. Each command
 * and query submitted to the cluster and each of their responses contains contextual information that helps servers
 * ensure operations are applied in the proper order and at the proper time. In the event that the session expires
 * or is closed, consistency guarantees are lost for operations submitted on the session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClientSession implements Session, Managed<Session> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSession.class);
  private static final double KEEP_ALIVE_RATIO = 0.4;

  /**
   * Client session state.
   */
  private enum State {
    OPEN,
    CLOSED,
    EXPIRED
  }

  private final Random random = new Random(System.currentTimeMillis());
  private final UUID clientId;
  private final Client client;
  private Address leader;
  private Set<Address> members;
  private final ThreadContext context;
  private final ConnectionStrategy connectionStrategy;
  private final SubmissionStrategy submissionStrategy;
  private List<Address> connectMembers;
  private Connection connection;
  private volatile State state = State.CLOSED;
  private volatile long id;
  private long timeout;
  private boolean recordFailures;
  private long failureTime;
  private CompletableFuture<Connection> connectFuture;
  private Scheduled retry;
  private final List<Consumer<Throwable>> retries = new ArrayList<>();
  private Scheduled keepAliveFuture;
  private final Map<String, Listeners<Object>> eventListeners = new ConcurrentHashMap<>();
  private final Set<Consumer<Session>> openListeners = new CopyOnWriteArraySet<>();
  private final Set<Consumer<Session>> closeListeners = new CopyOnWriteArraySet<>();
  private final Map<Long, Runnable> responses = new ConcurrentHashMap<>();
  private long commandRequest;
  private long commandResponse;
  private long requestSequence;
  private long responseSequence;
  private long responseIndex;
  private long eventIndex;
  private long completeIndex;

  public ClientSession(UUID clientId, Transport transport, Collection<Address> members, Serializer serializer, ConnectionStrategy connectionStrategy, SubmissionStrategy submissionStrategy) {
    this.clientId = Assert.notNull(clientId, "clientId");
    this.client = Assert.notNull(transport, "transport").client();
    this.members = new HashSet<>(Assert.notNull(members, "members"));
    this.context = new SingleThreadContext("copycat-client-" + clientId.toString(), Assert.notNull(serializer, "serializer").clone());
    this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.submissionStrategy = Assert.notNull(submissionStrategy, "submissionStrategy");
    this.connectMembers = submissionStrategy.getConnections(leader, new ArrayList<>(members));
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the session context.
   *
   * @return The session context.
   */
  public ThreadContext context() {
    return context;
  }

  /**
   * Sets the leader.
   */
  private boolean setLeader(Address leader) {
    if (this.leader == null && leader != null) {
      this.leader = leader;
      return true;
    } else if (this.leader != null && leader == null) {
      this.leader = null;
      return true;
    } else if (this.leader != null && !this.leader.equals(leader)) {
      this.leader = leader;
      return true;
    }
    return false;
  }

  /**
   * Sets the client remote members.
   */
  private void setMembers(Collection<Address> members) {
    this.members = new HashSet<>(members);
    this.connectMembers = submissionStrategy.getConnections(leader, new ArrayList<>(this.members));
  }

  /**
   * Sets the session timeout.
   */
  private void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  /**
   * Submits a command via the session.
   *
   * @param command The command to submit.
   * @param <T> The command output type.
   * @return A completable future to be completed with the command output.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (!isOpen()) {
        future.completeExceptionally(new IllegalStateException("session not open"));
        return;
      }

      CommandRequest request;
      if (command.consistency() == Command.ConsistencyLevel.NONE) {
        request = CommandRequest.builder()
          .withSession(id)
          .withSequence(0)
          .withCommand(command)
          .build();
      } else {
        request = CommandRequest.builder()
          .withSession(id)
          .withSequence(++commandRequest)
          .withCommand(command)
          .build();
      }

      submit(request, future);
    });
    return future;
  }

  /**
   * Recursively submits a command.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(CommandRequest request, CompletableFuture<T> future) {
    if (!isOpen()) {
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    long sequence = ++requestSequence;

    this.<CommandRequest, CommandResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        long responseSequence = request.sequence();
        sequenceResponse(sequence, () -> {
          commandResponse = responseSequence;
          completeResponse(response, future);
        });
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Submits a query via the session.
   *
   * @param query The query to submit.
   * @param <T> The query output type.
   * @return A completable future to be completed with the query output.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (!isOpen()) {
        future.completeExceptionally(new IllegalStateException("session not open"));
        return;
      }

      QueryRequest request;
      if (query.consistency() == Query.ConsistencyLevel.CAUSAL) {
        request = QueryRequest.builder()
          .withSession(id)
          .withSequence(commandResponse)
          .withIndex(responseIndex)
          .withQuery(query)
          .build();
      } else {
        request = QueryRequest.builder()
          .withSession(id)
          .withSequence(commandRequest)
          .withIndex(responseIndex)
          .withQuery(query)
          .build();
      }

      submit(request, future);
    });
    return future;
  }

  /**
   * Recursively submits a query.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(QueryRequest request, CompletableFuture<T> future) {
    if (!isOpen()) {
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    long sequence = ++requestSequence;

    this.<QueryRequest, QueryResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        // If the query consistency level is CAUSAL, we can simply complete queries in sequential order.
        if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL) {
          sequenceResponse(sequence, () -> {
            responseIndex = Math.max(responseIndex, response.index());
            completeResponse(response, future);
          });
        }
        // If the query consistency level is strong, the query must be executed sequentially. In order to ensure responses
        // are received in a sequential manner, we compare the response index number with the highest index for which
        // we've received a response and resubmit queries with output resulting from stale (prior) versions.
        else {
          sequenceResponse(sequence, () -> {
            if (response.index() > 0 && response.index() < responseIndex) {
              submit(request, future);
            } else {
              responseIndex = Math.max(responseIndex, response.index());
              completeResponse(response, future);
            }
          });
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Sequences a query response.
   */
  private void sequenceResponse(long sequence, Runnable callback) {
    // If the response is for the next sequence number (the response is received in order),
    // complete the future as appropriate. Note that some prior responses may have been received
    // out of order, so once this response is completed, complete any following responses that
    // are in sequence.
    if (sequence == responseSequence + 1) {
      responseSequence++;

      callback.run();

      // Iterate through responses in sequence if available and trigger completion callbacks that are in sequence.
      while (responses.containsKey(responseSequence + 1)) {
        responses.remove(++responseSequence).run();
      }
    } else {
      responses.put(sequence, callback);
    }
  }

  /**
   * Completes the given operation response.
   */
  @SuppressWarnings("unchecked")
  private void completeResponse(OperationResponse response, CompletableFuture future) {
    if (response.status() == Response.Status.OK) {
      future.complete(response.result());
      resetMembers();
    } else {
      future.completeExceptionally(response.error().createException());
    }
  }

  /**
   * Sends a session request.
   *
   * @param request The request to send.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return A completable future to be completed once the response is received.
   */
  private <T extends SessionRequest<T>, U extends SessionResponse<U>> CompletableFuture<U> request(T request) {
    if (!isOpen())
      return Futures.exceptionalFutureAsync(new IllegalStateException("session not open"), context.executor());
    return request(request, 1, new CompletableFuture<U>(), true);
  }

  /**
   * Sends a request to the cluster.
   * <p>
   * This method provides the primary interface to sending all requests to the Copycat cluster. All requests
   * are attempted until a failure event is detected.
   * <p>
   * The logic for managing connections is as follows:
   * When a new connection to the cluster is being established, the {@link SubmissionStrategy} is queried to get
   * a list of servers to which this client can send requests. The client will then attempt to connect to each
   * server in the list. If all connection attempts fail, the client will continue to attempt to reconnect to the
   * cluster until the session timeout expires. In the event that the client is able to successfully connect to
   * any server in the cluster but cannot verify its connection with a {@link ConnectRequest} or successfully
   * submit {@link KeepAliveRequest}s, the client will use the configured {@link ConnectionStrategy} to determine
   * how to handle those failures.
   *
   * @param request The request to send.
   * @param future The future to complete once the response is received.
   * @param checkOpen Whether to check if the session is open.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return The provided future to be completed once the response is received.
   */
  private <T extends Request<T>, U extends Response<U>> CompletableFuture<U> request(T request, int attempt, CompletableFuture<U> future, boolean checkOpen) {
    context.checkThread();

    // If the session already expired, immediately fail the future.
    if (checkOpen && !isOpen()) {
      future.completeExceptionally(new IllegalStateException("session expired"));
      return future;
    }

    // If we're already connected to a server, use the existing connection. The connection will be reset in the event
    // of an error on any connection, or callers can reset connections as well.
    if (connection != null) {
      return request(request, attempt, connection, future, checkOpen);
    }

    // If we've run out of servers to which to attempt to connect, determine whether we should expire the
    // session based on the responses from servers with which we did successfully communicate and the
    // time we were last able to successfully communicate with a correct server process. The failureTime
    // indicates the first time we received a NO_LEADER_ERROR from a server.
    if (connectMembers.isEmpty()) {
      // If the client failed to connect to any server then determine whether to attempt to connect again
      // or to expire the session. If the session is open and its timeout has elapsed, close the session,
      // otherwise automatically attempt to reconnect again.
      if (failureTime > 0) {
        // If the session is open, determine whether the session timeout has expired.
        if (id > 0) {
          // If the session timeout has expired, close the session.
          if (failureTime + timeout < System.currentTimeMillis()) {
            LOGGER.warn("Lost session");
            resetConnection().onExpire();
            future.completeExceptionally(new IllegalStateException("session expired"));
          }
          // If the session timeout has not expired, attempt to maintain the client's session by retrying
          // after a short amount of time.
          else {
            if (retry == null)
              retry = context.schedule(Duration.ofMillis(200), ClientSession.this::retryRequests);
            retries.add(e -> {
              if (e == null) {
                LOGGER.debug("Retrying {}", request);
                request(request, attempt, future, checkOpen);
              } else {
                future.completeExceptionally(e);
              }
            });
          }
        }
        // If no session is registered for the client, use the connection strategy to perform retries.
        else {
          if (retry != null) {
            retries.add(e -> {
              if (e == null) {
                LOGGER.debug("Retrying {}", request);
                request(request, attempt + 1, future, checkOpen);
              } else {
                future.completeExceptionally(e);
              }
            });
          } else {
            fail(request, attempt, future, checkOpen);
          }
        }
      }
      // If the client was able to connect to the cluster but couldn't commit a command, use the connection
      // strategy to determine what to do.
      else {
        if (retry != null) {
          retries.add(e -> {
            if (e == null) {
              LOGGER.debug("Retrying {}", request);
              request(request, attempt + 1, future, checkOpen);
            } else {
              future.completeExceptionally(e);
            }
          });
        } else {
          fail(request, attempt, future, checkOpen);
        }
      }
      return future;
    }

    // We don't want concurrent requests to attempt to connect to the same server at the same time, so
    // if the connection is already being attempted, piggyback on the existing connect future.
    connect(future, checkOpen).whenComplete((connection, error) -> {
      if (!checkOpen || isOpen()) {
        if (error == null) {
          request(request, attempt, connection, future, checkOpen);
        } else {
          request(request, attempt, future, checkOpen);
        }
      } else {
        future.completeExceptionally(new IllegalStateException("session not open"));
      }
    });
    return future;
  }

  /**
   * Sends a session request to the given connection.
   *
   * @param request The request to send.
   * @param connection The connection to which to send the request.
   * @param future The future to complete once the response is received.
   * @param checkOpen Whether to check if the session is open.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return The provided future to be completed once the response is received.
   */
  private <T extends Request<T>, U extends Response<U>> CompletableFuture<U> request(T request, int attempt, Connection connection, CompletableFuture<U> future, boolean checkOpen) {
    LOGGER.debug("Sending {}", request);
    connection.<T, U>send(request).whenComplete((response, error) -> {
      if (!checkOpen || isOpen()) {
        if (error == null) {
          LOGGER.debug("Received {}", response);

          // If the response is an error response, check if the session state has changed.
          if (response.status() == Response.Status.ERROR) {
            // If the response error is a no leader error, reset the connection and send another request.
            if (response.error() == RaftError.Type.NO_LEADER_ERROR) {
              resetConnection().request(request, attempt, future, checkOpen);
            }
            // If the response error is an unknown session error, immediately expire the session.
            else if (response.error() == RaftError.Type.UNKNOWN_SESSION_ERROR) {
              resetConnection().onExpire();
              future.completeExceptionally(new IllegalStateException("session expired"));
            }
            // If the response error is an application or internal error, immediately complete the future.
            else if (response.error() == RaftError.Type.APPLICATION_ERROR
              || response.error() == RaftError.Type.INTERNAL_ERROR) {
              resetFailureTime();
              future.completeExceptionally(response.error().createException());
            }
            // If we've made it this far, for all other error types log the failure time and attempt
            // to send the request to another server.
            else {
              resetConnection().request(request, attempt, future, checkOpen);
            }
          }
          // If the response status is OK, reset the failure time and complete the future.
          else {
            resetMembers();
            future.complete(response);
          }
        }
        // If an error occurred, set the failure time and attempt to contact the next server recursively.
        else {
          LOGGER.debug("{}", error.getMessage());
          resetConnection().request(request, attempt, future, checkOpen);
        }
      } else {
        future.completeExceptionally(new IllegalStateException("session not open"));
      }
    });
    return future;
  }

  /**
   * Establishes a connection to the next member.
   */
  private CompletableFuture<Connection> connect(CompletableFuture<?> future, boolean checkOpen) {
    // Remove the next random member from the members list.
    Address member = connectMembers.remove(random.nextInt(connectMembers.size()));
    boolean recordFailures = this.recordFailures;
    this.recordFailures = false;

    // Connect to the server. If the connection fails, recursively attempt to connect to the next server,
    // otherwise setup the connection and send the request.
    if (connectFuture == null) {
      // If there's no existing connect future, create a new one.
      LOGGER.debug("Connecting to {}", member);
      connectFuture = new CompletableFuture<>();

      // Connect to the server.
      client.connect(member).whenComplete((connection, connectError) -> {
        if (!checkOpen || isOpen()) {
          if (connectError == null) {
            // If the connection to the server was successful, reset the failure time.
            resetFailureTime();

            // Register the connection to the server.
            setupConnection(connection, recordFailures).whenComplete((setupResult, setupError) -> {
              if (!checkOpen || isOpen()) {
                CompletableFuture<Connection> connectFuture = this.connectFuture;
                this.connectFuture = null;

                // If the connection was successfully registered, complete the connection.
                if (setupError == null) {
                  resetFailureTime();
                  connectFuture.complete(connection);
                }
                // If the client failed to register the connection, reset the connection and
                // complete the attempt exceptionally.
                else {
                  LOGGER.debug("Failed to setup connection to {}", member.socketAddress());
                  if (recordFailures) {
                    setFailureTime();
                  }
                  resetConnection();
                  connectFuture.completeExceptionally(setupError);
                }
              } else {
                future.completeExceptionally(new IllegalStateException("session not open"));
              }
            });
          } else {
            // If the connection to the server failed, record the failure time if necessary.
            LOGGER.debug("Failed to connect to {}", member.socketAddress());
            if (recordFailures) {
              setFailureTime();
            }

            // Complete the connection future exceptionally.
            CompletableFuture<Connection> connectFuture = this.connectFuture;
            this.connectFuture = null;
            connectFuture.completeExceptionally(connectError);
          }
        } else {
          future.completeExceptionally(new IllegalStateException("session not open"));
        }
      });
    }
    return connectFuture;
  }

  /**
   * Handles the failure of a connection.
   */
  private <T extends Request<T>, U extends Response<U>> void fail(T request, int attempt, CompletableFuture<U> future, boolean checkOpen) {
    connectionStrategy.attemptFailed(new ConnectionStrategy.Attempt() {
      @Override
      public int attempt() {
        return attempt;
      }

      @Override
      public void fail() {
        future.completeExceptionally(new ConnectException());
      }

      @Override
      public void retry() {
        resetConnection().resetMembers().request(request, attempt + 1, future, checkOpen);
      }

      @Override
      public void retry(Duration after) {
        if (retry == null)
          retry = context.schedule(after, ClientSession.this::retryRequests);
        retries.add(e -> {
          if (e == null) {
            LOGGER.debug("Retrying {}", request);
            request(request, attempt + 1, future, checkOpen);
          } else {
            future.completeExceptionally(e);
          }
        });
      }
    });
  }

  /**
   * Sets up the given connection.
   */
  private CompletableFuture<Connection> setupConnection(Connection connection, boolean recordFailures) {
    this.connection = connection;
    connection.closeListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });
    connection.exceptionListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });
    connection.handler(PublishRequest.class, this::handlePublish);

    // When we first connect to a new server, first send a ConnectRequest to the server if the
    // session has already been registered. Once the connection has been established, send a KeepAlive
    // request to the new server. This will allow the new server to immediately provide membership info
    // to the client.
    if (id != 0) {
      ConnectRequest request = ConnectRequest.builder()
        .withSession(id)
        .build();

      CompletableFuture<Connection> future = new CompletableFuture<>();
      connection.<ConnectRequest, ConnectResponse>send(request).whenComplete((connectResponse, connectError) -> {
        if (isOpen()) {
          if (connectError == null) {
            // If the connection was successfully created, immediately send a keep-alive request
            // to the server to ensure we maintain our session and get an updated list of server addresses.
            if (connectResponse.status() == Response.Status.OK) {
              keepAlive().whenComplete((keepAliveResult, keepAliveError) -> {
                future.complete(connection);
              });
            } else {
              future.completeExceptionally(connectResponse.error().createException());
            }
          } else {
            future.completeExceptionally(connectError);
          }
        }
      });
      return future;
    }
    return CompletableFuture.completedFuture(connection);
  }

  /**
   * Retries sending requests.
   */
  protected synchronized void retryRequests() {
    retry = null;
    List<Consumer<Throwable>> retries = new ArrayList<>(this.retries);
    this.retries.clear();
    resetMembers();
    for (Consumer<Throwable> retry : retries) {
      retry.accept(null);
    }
  }

  /**
   * Resets the current connection.
   */
  private ClientSession resetConnection() {
    connection = null;
    return this;
  }

  /**
   * Resets the members to which to connect.
   */
  private ClientSession resetMembers() {
    if (connectMembers.isEmpty() || connectMembers.size() < members.size() - 1) {
      connectMembers = submissionStrategy.getConnections(leader, new ArrayList<>(members));
      recordFailures = true;
    }
    return this;
  }

  /**
   * Sets the failure time if not already set.
   */
  private ClientSession setFailureTime() {
    if (failureTime == 0) {
      failureTime = System.currentTimeMillis();
    }
    return this;
  }

  /**
   * Resets the failure timeout.
   */
  private ClientSession resetFailureTime() {
    failureTime = 0;
    return this;
  }

  /**
   * Registers the session.
   */
  private CompletableFuture<Void> register() {
    context.checkThread();

    RegisterRequest request = RegisterRequest.builder()
      .withClient(clientId)
      .build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    this.<RegisterRequest, RegisterResponse>request(request, 1, new CompletableFuture<>(), false).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          setLeader(response.leader());
          setMembers(response.members());
          setTimeout(response.timeout());
          onOpen(response.session());
          resetConnection().resetMembers().keepAlive(Duration.ofMillis(Math.round(response.timeout() * KEEP_ALIVE_RATIO)));
          future.complete(null);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Sends a keep alive request.
   */
  private CompletableFuture<Void> keepAlive() {
    context.checkThread();

    CompletableFuture<Void> future = new CompletableFuture<>();
    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(id)
      .withCommandSequence(commandResponse)
      .withEventIndex(completeIndex)
      .build();

    boolean recordFailures = this.recordFailures;
    this.<KeepAliveRequest, KeepAliveResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          setLeader(response.leader());
          setMembers(response.members());
          resetMembers();
          future.complete(null);
        } else if (isOpen()) {
          if (recordFailures) {
            setFailureTime();
          }
          if (setLeader(response.leader())) {
            resetMembers();
          }
          future.complete(null);
        }
      } else if (isOpen()) {
        if (recordFailures) {
          setFailureTime();
        }
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Sends and reschedules keep alive request.
   */
  private void keepAlive(Duration interval) {
    keepAlive().whenComplete((result, error) -> {
      keepAliveFuture = context.schedule(interval, () -> {
        if (isOpen()) {
          context.checkThread();
          keepAlive(interval);
        }
      });
    });
  }

  /**
   * Triggers opening the session.
   */
  private void onOpen(long sessionId) {
    LOGGER.debug("Registered session: {}", sessionId);
    this.id = sessionId;
    this.eventIndex = id;
    this.state = State.OPEN;
    for (Consumer<Session> listener : openListeners) {
      listener.accept(this);
    }
  }

  @Override
  public CompletableFuture<Session> open() {
    CompletableFuture<Session> future = new CompletableFuture<>();
    context.executor().execute(() -> register().whenComplete((result, error) -> {
      if (error == null) {
        this.state = State.OPEN;
        future.complete(this);
      } else {
        future.completeExceptionally(error);
      }
    }));
    return future;
  }

  @Override
  public boolean isOpen() {
    return state == State.OPEN;
  }

  @Override
  public Listener<Session> onOpen(Consumer<Session> listener) {
    Listener<Session> wrapper = new SessionListener(Assert.notNull(listener, "listener"));
    openListeners.add(wrapper);
    return wrapper;
  }

  @Override
  public Session publish(String event) {
    return publish(event, null);
  }

  @Override
  public Session publish(String event, Object message) {
    Assert.notNull(event, "event");
    context.executor().execute(() -> {
      Listeners<Object> listeners = eventListeners.get(event);
      if (listeners != null) {
        listeners.accept(message);
      }
    });
    return this;
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    LOGGER.debug("{} - Received {}", id, request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.session() != id) {
      LOGGER.debug("{} - Inconsistent session ID: {}", id, request.session());
      return Futures.exceptionalFuture(new UnknownSessionException("incorrect session ID"));
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.previousIndex() != eventIndex) {
      LOGGER.debug("{} - Inconsistent event index: {}", id, request.previousIndex());
      return CompletableFuture.completedFuture(PublishResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withIndex(eventIndex)
        .build());
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    eventIndex = request.eventIndex();

    // For each event in the events batch, call the appropriate event listener and create a CompletableFuture
    // to be called once the event callback is complete. Futures will ensure that an event is not acknowledged
    // until all event callbacks have completed.
    List<CompletableFuture<Void>> futures = new ArrayList<>(request.events().size());
    for (Event<?> event : request.events()) {
      Listeners<Object> listeners = eventListeners.get(event.name());
      if (listeners != null) {
        futures.add(listeners.accept(event.message()));
      }
    }

    // Wait for all event listeners to complete and then respond to the event message. This ensures that
    // linearizable events are completed between their invocation and response. If the async queue is backed
    // up and we don't wait for callbacks to complete, the cluster will believe an event to have been received
    // and handled before it has indeed been received and handled.
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
      .handleAsync((result, error) -> {
        // Store the highest index for which event callbacks have completed.
        completeIndex = Math.max(completeIndex, request.eventIndex());

        return PublishResponse.builder()
          .withStatus(Response.Status.OK)
          .withIndex(eventIndex)
          .build();
      }, context.executor());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener onEvent(String event, Consumer listener) {
    return eventListeners.computeIfAbsent(Assert.notNull(event, "event"), e -> new Listeners<>())
      .add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (keepAliveFuture != null) {
        keepAliveFuture.cancel();
      }
      if (retry != null) {
        retry.cancel();
      }
      failRetries();

      CompletableFuture<Void> clientCloseFuture;
      try {
        clientCloseFuture = client.close();
      } catch (Exception e) {
        clientCloseFuture = CompletableFuture.completedFuture(null);
      }

      clientCloseFuture.whenComplete((result, error) -> {
        connection = null;
        if (isOpen()) {
          this.id = 0;
          this.state = State.CLOSED;
          closeListeners.forEach(l -> l.accept(this));
        }

        CompletableFuture.runAsync(() -> {
          try {
            context.close();
          } catch (Exception e) {
          }
          future.complete(null);
        });
      });
    });
    return future;
  }

  /**
   * Fails retries remaining in the client.
   */
  private synchronized void failRetries() {
    Throwable e = new IllegalStateException("session closed");
    for (Consumer<Throwable> retry : retries) {
      retry.accept(e);
    }
    retries.clear();
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED || state == State.EXPIRED;
  }

  @Override
  public Listener<Session> onClose(Consumer<Session> listener) {
    Listener<Session> wrapper = new SessionListener(Assert.notNull(listener, "listener"));
    closeListeners.add(wrapper);
    return wrapper;
  }

  /**
   * Handles expiring the session.
   */
  private void onExpire() {
    if (isOpen()) {
      LOGGER.debug("Expired session: {}", id);
      this.id = 0;
      this.state = State.EXPIRED;
      closeListeners.forEach(l -> l.accept(this));
    }
  }

  @Override
  public boolean isExpired() {
    return state == State.EXPIRED;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Session && ((Session) object).id() == id;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), id);
  }

  /**
   * Session listener holder.
   */
  private class SessionListener implements Listener<Session> {
    private final Consumer<Session> listener;

    private SessionListener(Consumer<Session> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(Session event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      closeListeners.remove(this);
    }
  }

}
