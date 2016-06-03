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

package com.linkedin.pinot.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class DataSizeTest {

  @Test
  public void testToBytes() {

    Assert.assertEquals(DataSize.toBytes("128MB"),
        128 * 1024 * 1024L);
    Assert.assertEquals(DataSize.toBytes("1024B"),
        1024L);

    Assert.assertEquals(DataSize.toBytes("1.5GB"),
        (long)(1.5 * 1024 * 1024 * 1024L));

    Assert.assertEquals(DataSize.toBytes("123"), -1);
    Assert.assertEquals(DataSize.toBytes("123PB"), -1);
    Assert.assertEquals(DataSize.toBytes("-123MB"), -1);
    Assert.assertEquals(DataSize.toBytes("12GB3GB"), -1);

    Assert.assertEquals(DataSize.toBytes("123kb"), 123 * 1024L);

    Assert.assertEquals(DataSize.toBytes("123tb"), 123 * 1024L * 1024 * 1024 * 1024);

  }

}
