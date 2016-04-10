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
package io.atomix.copycat.examples;

import io.atomix.copycat.Query;

/**
 * Value get query.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GetQuery implements Query<Object> {

  @Override
  public ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE_LEASE;
  }

}
