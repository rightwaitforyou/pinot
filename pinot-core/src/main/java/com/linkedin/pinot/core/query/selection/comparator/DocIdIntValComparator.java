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
package com.linkedin.pinot.core.query.selection.comparator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockSingleValIterator;

public class DocIdIntValComparator implements IDocIdValComparator{

  int orderToggleMultiplier = 1;
  private final BlockSingleValIterator blockValSetIterator;

  public DocIdIntValComparator(Block block, boolean ascending) {
    blockValSetIterator = (BlockSingleValIterator) block.getBlockValueSet().iterator();
    if (ascending) {
      orderToggleMultiplier = -1;
    }
  }

  public int compare(int docId1, int docId2) {
    blockValSetIterator.skipTo(docId1);
    int val1 = blockValSetIterator.nextIntVal();
    blockValSetIterator.skipTo(docId2);
    int val2 = blockValSetIterator.nextIntVal();
    return Integer.compare(val1, val2) * orderToggleMultiplier;
  }

}
