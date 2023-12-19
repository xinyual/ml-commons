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
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableList;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableSet;
import org.opensearch.script.Script;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.opensearch.ml.common.utils.StringUtils.gson;

@Log4j2
@ToolAnnotation(TraceAnalysisTool.TYPE)
public class TraceAnalysisTool implements Tool {
    public static final String TYPE = "TraceAnalysisTool";

    public static final List<String> RANGE_KEYS = ImmutableList.of("gt", "lt", "gte", "lte");

    private static final String START_TIME_KEY = "startTime";

    private static final String END_TIME_KEY = "endTIme";

    public static final String SORT_KEY = "sort";

    public static final Set<String> LOGIC_KEY_SET = ImmutableSet.of("or_filters", "and_filters", "not_filters");

    private static final String AGGREGATE_FIELD_KEY = "requestedEntity";

    private static final String DEFAULT_AGGS_NAME = "aggs_result";

    private static final String AGGS_TYPE_KEY = "aggType";

    private static final String AGGS_TYPE_TREND = "trend";

    private static final String INTERVAL_KEY = "interval";

    private static final String METRIC_TYPE_KEY = "metricType";

    private static final String METRIC_NAME = "metricName";

    private static final String LATENCY_NAME = "durationInNanos";

    private static final String THROUGHPUT_NAME = "traceId";

    private static final String AGG_COMPARATOR = "aggComparator";

    private static final String AGG_TO_COMPARE = "aggToCompare";

    private static final int DEFAULT_SIZE = 10;

    private enum MetricName{
        latency,
        throughput;

        private static MetricName from(String value)
        {
            try {
                return MetricName.valueOf(value.toLowerCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Wrong Metric Type");
            }
        }
    }

    private enum MetricType{
        max,
        min,
        avg,
        sum;

        private static MetricType from(String value)
        {
            try {
                return MetricType.valueOf(value.toLowerCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Wrong Metric Type");
            }
        }
    }

    private enum AggRankType{
        top,
        bottom;

        private static AggRankType from(String value)
        {
            try {
                return AggRankType.valueOf(value.toLowerCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Wrong Aggregation Rank Type");
            }
        }

        private static boolean RankTypeToBool(AggRankType aggRankType)
        {
            if (aggRankType == AggRankType.top)
            {
                return true;
            }
            return false;
        }
    }

    private Client client;

    private static final String DEFAULT_DESCRIPTION = "Use this tool to generate DSL for trace analysis and execute.";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;



    public TraceAnalysisTool(Client client) {
        this.client = client;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        Map<String, Object> returnParameters = gson.fromJson(parameters.get("input"), Map.class);
        String indexName = parameters.get("indexName");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // add logical filter

        boolQueryBuilder = addLogicFilter(returnParameters, boolQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        //add sort
        if (returnParameters.containsKey(SORT_KEY))
        {
            searchSourceBuilder = addSort((Map<String, String>) returnParameters.get(SORT_KEY), searchSourceBuilder);
        }

        // add aggregate
        if (returnParameters.containsKey(AGGREGATE_FIELD_KEY)){
            searchSourceBuilder = addAggs(returnParameters, searchSourceBuilder);
        }

        SearchRequest searchRequest = new SearchRequest(new String[]{indexName}, searchSourceBuilder);
        client.search(searchRequest, ActionListener.<SearchResponse>wrap(searchResponse -> {
            SearchResponseSections internalResponse = searchResponse.getInternalResponse();
            Aggregations aggregations = internalResponse.aggregations();
            List<Aggregation> aggregationList = postProcessing(aggregations.asList(), returnParameters);
            SearchResponseSections processedInternalResponse = new SearchResponseSections(
                    internalResponse.hits(),
                    new Aggregations(aggregationList),
                    internalResponse.suggest(),
                    internalResponse.timedOut(),
                    internalResponse.terminatedEarly(),
                    new SearchProfileShardResults(internalResponse.profile()),
                    internalResponse.getNumReducePhases()
            );
            SearchResponse processedSearchResponse = new SearchResponse(
                    processedInternalResponse,
                    searchResponse.getScrollId(),
                    searchResponse.getTotalShards(),
                    searchResponse.getSuccessfulShards(),
                    searchResponse.getSkippedShards(),
                    searchResponse.getTook().getMillis(),
                    searchResponse.getShardFailures(),
                    searchResponse.getClusters()
            );
            listener.onResponse((T) processedSearchResponse);
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
            return new TraceAnalysisTool(client);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }


    private List<QueryBuilder> extractLogicQuery(List<Map<String, Object>> parameters)
    {
        List<QueryBuilder> logicQueryBuilderList = new ArrayList<>();
        for (Map<String, Object> it: parameters)
        {
            String targetField = (String) it.get("name");
            Object value = it.get("value");
            if (it.containsKey("gt") || it.containsKey("lt"))
            {
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(targetField);
                if (it.containsKey("gt"))
                {
                    rangeQueryBuilder.gt(it.get("gt"));
                }
                if (it.containsKey("gte"))
                {
                    rangeQueryBuilder.gt(it.get("gte"));
                }
                if (it.containsKey("lt"))
                {
                    rangeQueryBuilder.gt(it.get("lt"));
                }
                if (it.containsKey("lte"))
                {
                    rangeQueryBuilder.gt(it.get("lte"));
                }
                logicQueryBuilderList.add(rangeQueryBuilder);
            }
            else if (targetField.equals(START_TIME_KEY)){
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(START_TIME_KEY);
                rangeQueryBuilder.gt(parseDate((String) value));
                logicQueryBuilderList.add(rangeQueryBuilder);
            }
            else if(targetField.equals(END_TIME_KEY)){
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(START_TIME_KEY);
                rangeQueryBuilder.lt(parseDate((String) value));
                logicQueryBuilderList.add(rangeQueryBuilder);
            }
            else if (value instanceof Long){
                logicQueryBuilderList.add(QueryBuilders.termQuery(targetField, value));
            }
            else if (value instanceof String) {
                logicQueryBuilderList.add(QueryBuilders.matchQuery(targetField, value));
            }
        }
        return logicQueryBuilderList;
    }

    private BoolQueryBuilder addLogicFilter(Map<String, Object> parameters, BoolQueryBuilder boolQueryBuilder){
        Map<String, Consumer<QueryBuilder>> actions = Map.of(
                "andFilters", boolQueryBuilder::filter,
                "orFilters", boolQueryBuilder::should,
                "notFilters", boolQueryBuilder::mustNot
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

    private SearchSourceBuilder addAggs(Map<String, Object> parameters, SearchSourceBuilder searchSourceBuilder)
    {
        String aggType = (String) parameters.get(AGGS_TYPE_KEY);
        if (aggType.equals(AGGS_TYPE_TREND))
        {
            searchSourceBuilder = addTrendAggs(parameters, searchSourceBuilder);
        }
        else if (aggType.contains("_"))
        {
            String [] rankParas = aggType.split("_");
            searchSourceBuilder = addRankAggs(parameters, searchSourceBuilder, rankParas);
        }
        else {
            searchSourceBuilder = addTermsAggs(parameters, searchSourceBuilder);
        }
        return searchSourceBuilder;
    }


    private SearchSourceBuilder addRankAggs(Map<String, Object> parameters, SearchSourceBuilder searchSourceBuilder, String[] rankParas)
    {

        AggRankType rankType = AggRankType.from(rankParas[0]);
        int rankNumber = Integer.parseInt(rankParas[1]);
        BucketOrder bucketOrder = BucketOrder.aggregation(getBucketOrderPath(parameters), AggRankType.RankTypeToBool(rankType));
        AggregationBuilder aggregationBuilder = buildSubAgg(parameters, rankNumber, bucketOrder);
        searchSourceBuilder.aggregation(aggregationBuilder);

        return searchSourceBuilder;
    }

    private String getBucketOrderPath(Map<String, Object> parameters)
    {
        MetricName metricName = MetricName.from((String) parameters.get(METRIC_NAME));
        if (metricName == MetricName.throughput)
        {
            return "throughput";
        }
        return "agg_latency";
    }

    private SearchSourceBuilder addTrendAggs(Map<String, Object> parameters, SearchSourceBuilder searchSourceBuilder)
    {
        AggregationBuilder historyAggregation = AggregationBuilders.histogram("trend").field(START_TIME_KEY).interval((double) parameters.get(INTERVAL_KEY));
        historyAggregation.subAggregation(buildSubAgg(parameters, DEFAULT_SIZE, null));
        searchSourceBuilder.aggregation(historyAggregation);
        return searchSourceBuilder;
    }

    private SearchSourceBuilder addTermsAggs(Map<String, Object> parameters, SearchSourceBuilder searchSourceBuilder)
    {
        AggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(DEFAULT_AGGS_NAME).field((String) parameters.get(AGGREGATE_FIELD_KEY));
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        return searchSourceBuilder;
    }

    private AggregationBuilder buildSubAgg(Map<String, Object> parameters, int size, BucketOrder bucketOrder)
    {
        String targetField = (String) parameters.get(AGGREGATE_FIELD_KEY);
        AggregationBuilder demo = AggregationBuilders.terms("demo");
        AggregationBuilder aggregationBuilder;
        if (bucketOrder ==null)
        {
            aggregationBuilder = AggregationBuilders.terms(DEFAULT_AGGS_NAME).field(targetField).size(size);
        }
        else {
            aggregationBuilder = AggregationBuilders.terms(DEFAULT_AGGS_NAME).field(targetField).size(size).order(bucketOrder);
        }
        MetricName metricName = MetricName.from((String) parameters.get(METRIC_NAME));
        Object metricType = parameters.get(METRIC_TYPE_KEY);
        AggregationBuilder subAggregation = null;
        if (metricName == MetricName.latency) {
            if (metricType instanceof Double) {
                subAggregation = AggregationBuilders.percentiles(LATENCY_NAME).field(LATENCY_NAME).percentiles((Double) metricType);
            } else {
                switch (MetricType.from((String) metricType)) {
                    case max:
                        subAggregation = AggregationBuilders.max("agg_latency").field(LATENCY_NAME);
                        break;
                    case min:
                        subAggregation = AggregationBuilders.min("agg_latency").field(LATENCY_NAME);
                        break;
                    case sum:
                        subAggregation = AggregationBuilders.sum("agg_latency").field(LATENCY_NAME);
                        break;
                    default:
                        subAggregation = AggregationBuilders.avg("agg_latency").field(LATENCY_NAME);
                }
            }
        }
        else if (metricName == MetricName.throughput)
        {
            subAggregation = AggregationBuilders.count("throughput").field(THROUGHPUT_NAME);
        }
        aggregationBuilder.subAggregation(subAggregation);
        return aggregationBuilder;
    }


    private List<Aggregation> postProcessing(List<Aggregation> aggregationList, Map<String, Object> parameters)
    {
        List<Aggregation> finalAggregationList = new ArrayList<>();
        String aggType = (String) parameters.get(AGGS_TYPE_KEY);

        for (Aggregation aggregation: aggregationList)
        {
            if (aggType.equals("trend"))
            {
                finalAggregationList.add(postTrendAggregation((InternalHistogram) aggregation, parameters));
            }
            else {
                finalAggregationList.add(postRankAggregation((StringTerms) aggregation, parameters));
            }
        }
        return finalAggregationList;
    }

    private StringTerms postRankAggregation(StringTerms stringTerms, Map<String, Object> parameters)
    {
        List<StringTerms.Bucket> stringTermsBuckets = stringTerms.getBuckets();
        List<StringTerms.Bucket> processedStringTermsBuckets = new ArrayList<>();
        for (StringTerms.Bucket bucket: stringTermsBuckets)
        {
            bucket = processDedicateBucket(stringTerms, bucket, parameters);
            if (filterBucket(bucket, parameters))
            {
                processedStringTermsBuckets.add(bucket);
            }
        }
        StringTerms processedStringTerms = stringTerms.create(processedStringTermsBuckets);
        return processedStringTerms;
    }

    private InternalHistogram postTrendAggregation(InternalHistogram internalHistogram, Map<String, Object> parameters)
    {
        List<InternalHistogram.Bucket> internalHistogramBuckets = internalHistogram.getBuckets();
        List<InternalHistogram.Bucket> processedInternalHistogramBuckets = new ArrayList<>();
        for (InternalHistogram.Bucket internalHistogramBucket: internalHistogramBuckets)
        {
            List<Aggregation> stringTermAggs = internalHistogramBucket.getAggregations().asList();
            List<Aggregation> processedStringTermAggs = new ArrayList<>();
            for (Aggregation aggregation: stringTermAggs)
            {
                StringTerms processedStringTerms = postRankAggregation((StringTerms) aggregation, parameters);
                processedStringTermAggs.add(processedStringTerms);
            }
        }
        InternalHistogram processedInternalHistogram = internalHistogram.create(processedInternalHistogramBuckets);
        return processedInternalHistogram;
    }

    private boolean filterBucket(StringTerms.Bucket bucket, Map<String, Object> parameters)
    {
        if (parameters.containsKey(AGG_COMPARATOR) && parameters.containsKey(AGG_TO_COMPARE)) {
            double aggToCompare = (double) parameters.get(AGG_TO_COMPARE);
            Double value = ((InternalNumericMetricsAggregation.SingleValue) bucket.getAggregations().asList().get(0)).value();
            if (parameters.get(AGG_COMPARATOR).toString().equals("gt") && value > aggToCompare)
            {
                return true;
            }
            if (parameters.get(AGG_COMPARATOR).toString().equals("lt") && value < aggToCompare)
            {
                return true;
            }
            return false;
        }
        return true;
    }

    private StringTerms.Bucket processDedicateBucket(StringTerms stringTerms, StringTerms.Bucket bucket, Map<String, Object> parameters)
    {
        MetricType metricType = MetricType.from((String) parameters.get(METRIC_TYPE_KEY));
        InternalAggregations processedAggregations;
        MetricName metricName = MetricName.from((String) parameters.get(METRIC_NAME));
        InternalNumericMetricsAggregation.SingleValue original;
        InternalNumericMetricsAggregation.SingleValue processed;
        if (metricName == MetricName.latency) {
            switch (metricType) {
                case avg:
                    original = (InternalAvg) bucket.getAggregations().asList().get(0);
                    processed = new InternalAvg(
                            original.getName(),
                            ((InternalAvg) original).getValue() / getDivide(parameters),
                            1,
                            DocValueFormat.RAW,
                            original.getMetadata()
                    );
                    break;
                case max:
                    original = (InternalMax) bucket.getAggregations().asList().get(0);
                    processed = new InternalMax(
                            original.getName(),
                            ((InternalMax) original).getValue() / getDivide(parameters),
                            DocValueFormat.RAW,
                            original.getMetadata()
                    );
                    break;
                case min:
                    original = (InternalMin) bucket.getAggregations().asList().get(0);
                    processed = new InternalMin(
                            original.getName(),
                            ((InternalMin) original).getValue() / getDivide(parameters),
                            DocValueFormat.RAW,
                            original.getMetadata()
                    );
                    break;
                default:
                    original = (InternalSum) bucket.getAggregations().asList().get(0);
                    processed = new InternalSum(
                            original.getName(),
                            ((InternalSum) original).getValue() / getDivide(parameters),
                            DocValueFormat.RAW,
                            original.getMetadata()
                    );
            }

        }
        else {
            original = (InternalValueCount) bucket.getAggregations().asList().get(0);
            processed = new InternalAvg(
                    original.getName(),
                    ((InternalValueCount) original).getValue() / getDivide(parameters),
                    1,
                    DocValueFormat.RAW,
                    original.getMetadata()
            );
        }
        processedAggregations = InternalAggregations.from(ImmutableList.of(processed));
        return stringTerms.createBucket(processedAggregations, bucket);
    }

    private double getDivide(Map<String, Object> parameters)
    {
        MetricName metricName = MetricName.from((String) parameters.get(METRIC_NAME));
        double divideNum;
        if (metricName == MetricName.latency)
        {
            divideNum = 1000_000;
        }
        else {
            LocalDateTime dateTime1 = LocalDateTime.of(2023, 1, 1, 12, 0, 0); // Example date-time
            LocalDateTime dateTime2 = LocalDateTime.now();
            divideNum = ChronoUnit.SECONDS.between(dateTime1, dateTime2);
        }
        return divideNum;
    }


    private Date parseDate(String dateString)
    {
        Date date = Date.from(Instant.parse(dateString));
        return date;
    }

}
