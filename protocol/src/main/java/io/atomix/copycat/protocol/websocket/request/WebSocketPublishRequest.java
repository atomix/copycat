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
package io.atomix.copycat.protocol.websocket.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.session.Event;

import java.util.List;

/**
 * Web socket publish request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketPublishRequest extends PublishRequest implements WebSocketRequest<WebSocketPublishRequest> {
  private final long id;

  @JsonCreator
  public WebSocketPublishRequest(
    @JsonProperty("id") long id,
    @JsonProperty("session") long session,
    @JsonProperty("eventIndex") long eventIndex,
    @JsonProperty("previousIndex") long previousIndex,
    @JsonProperty("events") List<Event<?>> events) {
    super(session, eventIndex, previousIndex, events);
    this.id = id;
  }

  @Override
  @JsonGetter("id")
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.PUBLISH;
  }

  /**
   * Returns the request type name.
   *
   * @return The request type name.
   */
  @JsonGetter("type")
  private String typeName() {
    return type().name();
  }

  @Override
  @JsonGetter("session")
  public long session() {
    return super.session();
  }

  @Override
  @JsonGetter("eventIndex")
  public long eventIndex() {
    return super.eventIndex();
  }

  @Override
  @JsonGetter("previousIndex")
  public long previousIndex() {
    return super.previousIndex();
  }

  @Override
  @JsonGetter("events")
  public List<Event<?>> events() {
    return super.events();
  }

  /**
   * Web socket publish request builder.
   */
  public static class Builder extends PublishRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public PublishRequest copy(PublishRequest request) {
      return new WebSocketPublishRequest(id, request.session(), request.eventIndex(), request.previousIndex(), request.events());
    }

    @Override
    public PublishRequest build() {
      return new WebSocketPublishRequest(id, session, eventIndex, previousIndex, events);
    }
  }
}
