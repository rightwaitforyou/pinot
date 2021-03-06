/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.data.generator;

import java.util.Random;
import java.lang.Math;

public class RangeLongGenerator implements Generator {
  private final long _start;
  private final long _end;
  private final long _delta;

  Random _randGen = new Random(System.currentTimeMillis());

  public RangeLongGenerator(long r1, long r2) {
    _start = (r1 < r2) ? r1 : r2;
    _end =   (r1 > r2) ? r1 : r2;

    _delta = _end - _start;
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    return (_start + (Math.abs(_randGen.nextLong()) % _delta));
  }
}
