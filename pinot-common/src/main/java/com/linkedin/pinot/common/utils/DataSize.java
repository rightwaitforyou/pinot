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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility to convert human readable datsize strings like '4567GB', '128MB'
 * to bytes
 */
public class DataSize {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSize.class);

  static final Pattern STORAGE_VAL_PATTERN = Pattern.compile("([\\d.]+)([TGMK]?B)", Pattern.CASE_INSENSITIVE);

  static final Map<String, Long> MULTIPLIER;
  static {
    MULTIPLIER = new HashMap<>(4);
    MULTIPLIER.put("TB", 1024L * 1024 * 1024 * 1024L);
    MULTIPLIER.put("GB", 1024L * 1024 * 1024L);
    MULTIPLIER.put("MB", 1024 * 1024L);
    MULTIPLIER.put("KB", 1024L);
    MULTIPLIER.put("B", 1L);

  }

  /**
   * Convert human readable datasize to bytes
   * @param val string to parse
   * @return returns -1 in case of invalid value
   */
  public static long toBytes(String val) {
    Matcher matcher = STORAGE_VAL_PATTERN.matcher(val);
    if (! matcher.matches()) {
      return -1;
    }
    String number = matcher.group(1);
    long multiplier = MULTIPLIER.get(matcher.group(2).toUpperCase());
    BigDecimal bytes = new BigDecimal(number);
    return bytes.multiply(BigDecimal.valueOf(multiplier)).longValue();
  }
}
