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
package io.atomix.copycat.protocol.http.response;

import io.atomix.copycat.session.Event;

import java.util.List;

/**
 * Event response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class HttpEventResponse extends HttpResponse {
  private long session;
  private List<Event<?>> events;

  public long getSession() {
    return session;
  }

  public HttpEventResponse setSession(long session) {
    this.session = session;
    return this;
  }

  public List<Event<?>> getEvents() {
    return events;
  }

  public HttpEventResponse setEvents(List<Event<?>> events) {
    this.events = events;
    return this;
  }
}
