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
package io.atomix.copycat.protocol.http;

import io.atomix.copycat.protocol.ProtocolConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.http.handlers.RequestHandler;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.util.concurrent.Futures;
import io.atomix.copycat.util.concurrent.Listener;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * HTTP server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class HttpServerConnection implements ProtocolServerConnection {
  private ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> connectListener;
  private ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> registerListener;
  private ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> keepAliveListener;
  private ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> unregisterListener;
  private ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> commandListener;
  private ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> queryListener;
  private final Map<Long, Consumer<PublishRequest>> eventCallbacks = new ConcurrentHashMap<>();

  public HttpServerConnection(Router router, Collection<RequestHandler.Factory> handlers) {
    setupRoutes(router, handlers);
  }

  /**
   * Sets up routes.
   */
  private void setupRoutes(Router router, Collection<RequestHandler.Factory> handlers) {
    router.route().handler(BodyHandler.create());
    for (RequestHandler.Factory factory : handlers) {
      factory.create(this).register(router);
    }
  }

  @Override
  public ProtocolServerConnection onConnect(ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> listener) {
    this.connectListener = listener;
    return this;
  }

  /**
   * Handles an on connect request.
   */
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ConnectResponse.Builder builder) {
    if (connectListener != null) {
      return connectListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  @Override
  public ProtocolServerConnection onRegister(ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> listener) {
    this.registerListener = listener;
    return this;
  }

  /**
   * Handles an on register request.
   */
  public CompletableFuture<RegisterResponse> onRegister(RegisterRequest request, RegisterResponse.Builder builder) {
    if (connectListener != null) {
      return registerListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  @Override
  public ProtocolServerConnection onKeepAlive(ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> listener) {
    this.keepAliveListener = listener;
    return this;
  }

  /**
   * Handles an on keep alive request.
   */
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request, KeepAliveResponse.Builder builder) {
    if (connectListener != null) {
      return keepAliveListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  @Override
  public ProtocolServerConnection onUnregister(ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> listener) {
    this.unregisterListener = listener;
    return this;
  }

  /**
   * Handles an on unregister request.
   */
  public CompletableFuture<UnregisterResponse> onUnregister(UnregisterRequest request, UnregisterResponse.Builder builder) {
    if (connectListener != null) {
      return unregisterListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  @Override
  public ProtocolServerConnection onCommand(ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> listener) {
    this.commandListener = listener;
    return this;
  }

  /**
   * Handles an on command request.
   */
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request, CommandResponse.Builder builder) {
    if (connectListener != null) {
      return commandListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  @Override
  public ProtocolServerConnection onQuery(ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> listener) {
    this.queryListener = listener;
    return this;
  }

  /**
   * Handles an on register request.
   */
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request, QueryResponse.Builder builder) {
    if (connectListener != null) {
      return queryListener.onRequest(request, builder);
    }
    return Futures.exceptionalFuture(new IllegalStateException("no handler registered"));
  }

  /**
   * Registers a session event listener.
   */
  public void onEvent(long session, Consumer<PublishRequest> callback) {
    eventCallbacks.put(session, callback);
  }

  @Override
  public CompletableFuture<PublishResponse> publish(ProtocolRequestFactory<PublishRequest.Builder, PublishRequest> builder) {
    PublishRequest request = builder.build(new PublishRequest.Builder());
    Consumer<PublishRequest> callback = eventCallbacks.get(request.session());
    if (callback != null) {
      callback.accept(request);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    return null;
  }

  @Override
  public Listener<ProtocolConnection> closeListener(Consumer<ProtocolConnection> listener) {
    return null;
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }
}
