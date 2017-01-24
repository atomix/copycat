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

import io.atomix.copycat.Query;
import io.vertx.core.http.HttpMethod;

import java.util.Map;

/**
 * HTTP query.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class HttpQuery implements HttpOperation, Query<Object> {
  private final String path;
  private final Map<String, String> headers;
  private final String body;
  private final ConsistencyLevel consistency;

  public HttpQuery(String path, Map<String, String> headers, String body, ConsistencyLevel consistency) {
    this.path = path;
    this.headers = headers;
    this.body = body;
    this.consistency = consistency;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public String method() {
    return HttpMethod.GET.name();
  }

  @Override
  public Map<String, String> headers() {
    return headers;
  }

  @Override
  public String body() {
    return body;
  }

  @Override
  public ConsistencyLevel consistency() {
    return consistency;
  }
}
