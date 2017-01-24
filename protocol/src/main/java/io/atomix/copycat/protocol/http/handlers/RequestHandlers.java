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
package io.atomix.copycat.protocol.http.handlers;

import java.util.Arrays;
import java.util.Collection;

/**
 * HTTP request handlers.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class RequestHandlers {
  public static final RequestHandler.Factory CREATE_SESSION = new CreateSessionHandler.Factory();
  public static final RequestHandler.Factory KEEP_ALIVE_SESSION = new KeepAliveHandler.Factory();
  public static final RequestHandler.Factory CLOSE_SESSION = new CloseSessionHandler.Factory();
  public static final RequestHandler.Factory COMMAND = new CommandHandler.Factory();
  public static final RequestHandler.Factory QUERY = new QueryHandler.Factory();
  public static final RequestHandler.Factory EVENT = new EventHandler.Factory();

  public static final Collection<RequestHandler.Factory> ALL = Arrays.asList(
    CREATE_SESSION,
    KEEP_ALIVE_SESSION,
    CLOSE_SESSION,
    COMMAND,
    QUERY,
    EVENT
  );

  private RequestHandlers() {
  }
}
