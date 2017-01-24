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

import io.atomix.copycat.Operation;

import java.util.Map;

/**
 * HTTP operation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface HttpOperation extends Operation<Object> {

  /**
   * Returns the HTTP request path.
   *
   * @return The HTTP request path.
   */
  String path();

  /**
   * Returns the HTTP request method.
   *
   * @return The HTTP request method.
   */
  String method();

  /**
   * Returns the HTTP request headers.
   *
   * @return The HTTP request headers.
   */
  Map<String, String> headers();

  /**
   * Returns the HTTP request body.
   *
   * @return The HTTP request body.
   */
  String body();

}
