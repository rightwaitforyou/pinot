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
package com.linkedin.pinot.core.plan.maker;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByImplementationType;
import com.linkedin.pinot.core.plan.AggregationGroupByOperatorPlanNode;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationOperatorPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.BitHacks;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a huge
 * plan node which will take the segment and query, then do everything.
 *
 *
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);
  private static final String NEW_AGGREGATION_GROUPBY_STRING = "new.aggregation.groupby";
  private boolean _enableNewAggregationGroupByCfg = false;

  /**
   * Default constructor.
   */
  public InstancePlanMakerImplV2() {
  }

  /**
   * Constructor for usage when client requires to pass queryExecutorConfig to this class.
   * Sets flag to indicate whether to enable new implementation of AggregationGroupBy operator,
   * based on the queryExecutorConfig.
   *
   * @param queryExecutorConfig
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    _enableNewAggregationGroupByCfg = queryExecutorConfig.getConfig().getBoolean(NEW_AGGREGATION_GROUPBY_STRING, false);
    LOGGER.info("New AggregationGroupBy operator: {}", (_enableNewAggregationGroupByCfg) ? "Enabled" : "Disabled");
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    return makeInnerSegmentPlan(indexSegment, brokerRequest, false);
  }

  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest,
      boolean enableNewAggregationGroupBy) {

    if (brokerRequest.isSetAggregationsInfo()) {
      if (!brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        if (enableNewAggregationGroupBy) {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        } else {
          return new AggregationOperatorPlanNode(indexSegment, brokerRequest);
        }
      } else {
        // Aggregation GroupBy
        PlanNode aggregationGroupByPlanNode;

        if (isGroupKeyFitForLong(indexSegment, brokerRequest)) {
          // AggregationGroupByPlanNode is the new implementation of group-by aggregations, and is currently turned OFF.
          // Once all feature and perf testing is performed, the code will be turned ON, and this 'if' check will
          // be removed.
          if (enableNewAggregationGroupBy) {
            aggregationGroupByPlanNode = new AggregationGroupByPlanNode(indexSegment, brokerRequest);
          } else {
            aggregationGroupByPlanNode = new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                AggregationGroupByImplementationType.Dictionary);
          }
        } else {
          aggregationGroupByPlanNode = new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
              AggregationGroupByImplementationType.DictionaryAndTrie);
        }
        return aggregationGroupByPlanNode;
      }
    }
    // Only Selection
    if (brokerRequest.isSetSelections()) {
      final PlanNode selectionPlanNode = new SelectionPlanNode(indexSegment, brokerRequest);
      return selectionPlanNode;
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<SegmentDataManager> segmentDataManagers, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    final InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    List<IndexSegment> segments = new ArrayList<>();

    // Go over all the segments and check if new implementation of group-by can be enabled.
    boolean enableNewAggregationGroupBy = _enableNewAggregationGroupByCfg;
    boolean isGroupByQuery = (brokerRequest.getAggregationsInfo() != null) && brokerRequest.isSetGroupBy();

    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      IndexSegment segment = segmentDataManager.getSegment();
      if (isGroupByQuery && !isGroupKeyFitForLong(segment, brokerRequest)) {
        enableNewAggregationGroupBy = false;
      }
      segments.add(segment);
    }

    final CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, timeOutMs,
        enableNewAggregationGroupBy);
    rootNode.setPlanNode(combinePlanNode);

    for (IndexSegment segment : segments) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(segment, brokerRequest, enableNewAggregationGroupBy));
    }
    return new GlobalPlanImplV0(rootNode);
  }

  private boolean isGroupKeyFitForLong(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    int totalBitSet = 0;
    for (final String column : brokerRequest.getGroupBy().getColumns()) {
      Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();
      totalBitSet += BitHacks.findLogBase2(dictionary.length()) + 1;
    }
    if (totalBitSet > 64) {
      return false;
    }
    return true;
  }
}
