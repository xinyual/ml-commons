/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.engine.tools;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableSet;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.opensearch.ml.common.utils.StringUtils.gson;

@Log4j2
@ToolAnnotation(TraceAnalysisTool.TYPE)
public class TraceAnalysisTool implements Tool {
    public static final String TYPE = "TraceAnalysisTool";

    public static final String RANGE_START_KEY = "start";

    public static final String RANGE_END_KEY = "end";

    public static final String RANGE_KEY = "range";

    public static final String RANGE_FIELD_KEY = "range_field";

    public static final String TARGET_FILED = "target_field";

    public static final String TARGET_VALUE = "target_value";

    public static final String SORT_KEY = "sort";


    public static final Set<String> LOGIC_KEY_SET = ImmutableSet.of("or_filters", "and_filters", "not_filters");
    private Client client;

    private static final String DEFAULT_DESCRIPTION = "Use this tool to generate PPL and execute.";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;

    private String modelId;

    private String contextPrompt;

    public TraceAnalysisTool(Client client, String modelId, String contextPrompt) {
        this.client = client;
        this.modelId = modelId;
        this.contextPrompt = contextPrompt;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        Map<String, Object> returnParameters = gson.fromJson(parameters.get("input"), Map.class);
        String indexName = parameters.get("indexName");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // add must
        if (returnParameters.containsKey(RANGE_KEY)){
            boolQueryBuilder.must(buildRange((Map<String, Object>) returnParameters.get(RANGE_KEY)));
        }


        if (returnParameters.containsKey(TARGET_FILED))
        {
            boolQueryBuilder.must(QueryBuilders.matchQuery((String) returnParameters.get(TARGET_FILED), returnParameters.get(TARGET_VALUE)));
        }
        // add other logic

        boolQueryBuilder = addOtherLogic(returnParameters, boolQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        //add sort
        if (returnParameters.containsKey(SORT_KEY))
        {
            searchSourceBuilder = addSort((Map<String, String>) returnParameters.get(SORT_KEY), searchSourceBuilder);
        }
        SearchRequest searchRequest = new SearchRequest(new String[]{indexName}, searchSourceBuilder);
        client.search(searchRequest, ActionListener.<SearchResponse>wrap(searchResponse -> {
            listener.onResponse((T) searchResponse);
        }, e -> {
            log.info(e);
        }));
    }


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return name;
    }



    @Override
    public boolean validate(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return false;
        }
        return true;
    }

    public static class Factory implements Tool.Factory<TraceAnalysisTool> {
        private Client client;

        private static TraceAnalysisTool.Factory INSTANCE;
        public static TraceAnalysisTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (TraceAnalysisTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new TraceAnalysisTool.Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public TraceAnalysisTool create(Map<String, Object> map) {
            return new TraceAnalysisTool(client, (String)map.get("model_id"), (String)map.get("prompt"));
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }


    private RangeQueryBuilder buildRange(Map<String, Object> rangeParameters)
    {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery((String) rangeParameters.get(RANGE_FIELD_KEY));
        if (rangeParameters.containsKey(RANGE_START_KEY))
        {
            rangeQueryBuilder.gte(rangeParameters.get(RANGE_START_KEY));
        }
        if (rangeParameters.containsKey(RANGE_END_KEY))
        {
            rangeQueryBuilder.lte(rangeParameters.get(RANGE_END_KEY));
        }
        return rangeQueryBuilder;
    }
    private List<QueryBuilder> extractLogicQuery(List<Map<String, Object>> parameters)
    {
        List<QueryBuilder> logicQueryBuilderList = new ArrayList<>();
        for (Map<String, Object> it: parameters)
        {
            String key = (String) it.get("key");
            Object value = it.get("value");
            if (value instanceof Long){
                logicQueryBuilderList.add(QueryBuilders.termQuery((String) it.get("key"), it.get("value")));
            }
            else if (value instanceof String) {
                logicQueryBuilderList.add(QueryBuilders.matchQuery((String) it.get("key"), it.get("value")));
            }
        }
        return logicQueryBuilderList;
    }
    private BoolQueryBuilder addOtherLogic(Map<String, Object> parameters, BoolQueryBuilder boolQueryBuilder){
        Map<String, Consumer<QueryBuilder>> actions = Map.of(
                "and_filters", boolQueryBuilder::filter,
                "or_filters", boolQueryBuilder::should,
                "not_filters", boolQueryBuilder::mustNot
        );
        for (String logicKey: LOGIC_KEY_SET)
        {
            Consumer<QueryBuilder> action = actions.get(logicKey);
            if (parameters.containsKey(logicKey))
            {
                List<QueryBuilder> extracedQueyrList = extractLogicQuery((List<Map<String, Object>>) parameters.get(logicKey));
                for (QueryBuilder termQueryBuilder: extracedQueyrList){
                    action.accept(termQueryBuilder);
                }

            }
        }
        return boolQueryBuilder;
    }

    private SearchSourceBuilder addSort(Map<String, String> parameters, SearchSourceBuilder searchSourceBuilder)
    {
        for (Map.Entry<String, String> iterator: parameters.entrySet())
        {
            SortBuilder sortBuilder = new FieldSortBuilder(iterator.getKey());
            sortBuilder.order(SortOrder.fromString(iterator.getValue()));
            searchSourceBuilder.sort(sortBuilder);
        }
        return searchSourceBuilder;
    }

}
