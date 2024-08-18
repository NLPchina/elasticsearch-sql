package org.elasticsearch.plugin.nlpcn.client.handler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.elasticsearch._types.RequestBase;
import co.elastic.clients.elasticsearch._types.SearchType;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoDistanceQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoValidationMethod;
import co.elastic.clients.json.DelegatingDeserializer;
import co.elastic.clients.json.JsonEnums;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpSerializable;
import co.elastic.clients.json.ObjectDeserializer;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoTileGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * ActionHandler
 *
 * @author shiyuan
 * @version V1.0
 * @since 2024-08-04 16:48
 */
public abstract class ActionHandler<FromRequest extends ActionRequest, ToRequest extends RequestBase, FromResponse extends JsonpSerializable, ToResponse extends ActionResponse> {

    private static final Logger logger = LogManager.getLogger(ActionHandler.class);

    static {
        try {
            Field field = JsonEnums.Deserializer.class.getDeclaredField("lookupTable");
            field.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<String, GeoValidationMethod> lookupTable = (Map<String, GeoValidationMethod>) field.get(GeoValidationMethod._DESERIALIZER);
            for (GeoValidationMethod geoValidationMethod : GeoValidationMethod.values()) {
                lookupTable.put(geoValidationMethod.jsonValue().toUpperCase(Locale.ROOT), geoValidationMethod);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        ((ObjectDeserializer<?>) DelegatingDeserializer.unwrap(GeoDistanceQuery._DESERIALIZER)).add((builder, value) -> {
        }, JsonpDeserializer.stringDeserializer(), "ignore_unmapped");
    }

    protected final ElasticsearchClient client;
    protected final JsonpMapper jsonpMapper;
    private final XContentParserConfiguration xContentParserConfiguration;

    public ActionHandler(ElasticsearchClient client) {
        this.client = client;
        this.jsonpMapper = client._jsonpMapper();
        this.xContentParserConfiguration = XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(Arrays.asList(
                //new Entry(Aggregation.class, new ParseField(AdjacencyMatrixAggregationBuilder.NAME), (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(CompositeAggregationBuilder.NAME), (p, c) -> ParsedComposite.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(FilterAggregationBuilder.NAME), (p, c) -> ParsedFilter.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(FiltersAggregationBuilder.NAME), (p, c) -> ParsedFilters.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GeoHashGridAggregationBuilder.NAME), (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GeoTileGridAggregationBuilder.NAME), (p, c) -> ParsedGeoTileGrid.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GlobalAggregationBuilder.NAME), (p, c) -> ParsedGlobal.fromXContent(p, (String) c)),
                //new Entry(Aggregation.class, new ParseField(AutoDateHistogramAggregationBuilder.NAME), (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(DateHistogramAggregationBuilder.NAME), (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(HistogramAggregationBuilder.NAME), (p, c) -> ParsedHistogram.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(VariableWidthHistogramAggregationBuilder.NAME), (p, c) -> ParsedVariableWidthHistogram.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(MissingAggregationBuilder.NAME), (p, c) -> ParsedMissing.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(NestedAggregationBuilder.NAME), (p, c) -> ParsedNested.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(ReverseNestedAggregationBuilder.NAME), (p, c) -> ParsedReverseNested.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(IpRangeAggregationBuilder.NAME), (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(DateRangeAggregationBuilder.NAME), (p, c) -> ParsedDateRange.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GeoDistanceAggregationBuilder.NAME), (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(RangeAggregationBuilder.NAME), (p, c) -> ParsedRange.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalSampler.PARSER_NAME), (p, c) -> ParsedSampler.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(DoubleTerms.NAME), (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(LongRareTerms.NAME), (p, c) -> ParsedLongRareTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(LongTerms.NAME), (p, c) -> ParsedLongTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(SignificantLongTerms.NAME), (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(SignificantStringTerms.NAME), (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(StringRareTerms.NAME), (p, c) -> ParsedStringRareTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(StringTerms.NAME), (p, c) -> ParsedStringTerms.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(AvgAggregationBuilder.NAME), (p, c) -> ParsedAvg.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(CardinalityAggregationBuilder.NAME), (p, c) -> ParsedCardinality.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(ExtendedStatsAggregationBuilder.NAME), (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GeoBoundsAggregationBuilder.NAME), (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(GeoCentroidAggregationBuilder.NAME), (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalHDRPercentileRanks.NAME), (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalHDRPercentiles.NAME), (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(MaxAggregationBuilder.NAME), (p, c) -> ParsedMax.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(MedianAbsoluteDeviationAggregationBuilder.NAME), (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(MinAggregationBuilder.NAME), (p, c) -> ParsedMin.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(ScriptedMetricAggregationBuilder.NAME), (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(StatsAggregationBuilder.NAME), (p, c) -> ParsedStats.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(SumAggregationBuilder.NAME), (p, c) -> ParsedSum.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalTDigestPercentileRanks.NAME), (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalTDigestPercentiles.NAME), (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(TopHitsAggregationBuilder.NAME), (p, c) -> ParsedTopHits.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(ValueCountAggregationBuilder.NAME), (p, c) -> ParsedValueCount.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(WeightedAvgAggregationBuilder.NAME), (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalBucketMetricValue.NAME), (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c)),
                //new Entry(Aggregation.class, new ParseField(DerivativePipelineAggregationBuilder.NAME), (p, c) -> ParsedDerivative.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(ExtendedStatsBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(PercentilesBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(InternalSimpleValue.NAME), (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c)),
                new NamedXContentRegistry.Entry(InternalAggregation.class, new ParseField(StatsBucketPipelineAggregationBuilder.NAME), (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c))
                //new Entry(Aggregation.class, new ParseField(TimeSeriesAggregationBuilder.NAME), (p, c) -> ParsedTimeSeries.fromXContent(p, (String) c))
        ))).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
    }

    public abstract String getName();

    public ToResponse handle(FromRequest request) throws IOException {
        return convertResponse(doHandle(convertRequest(request)));
    }

    protected abstract FromResponse doHandle(ToRequest request) throws IOException;

    protected abstract ToRequest convertRequest(FromRequest request) throws IOException;

    protected abstract ToResponse convertResponse(FromResponse response) throws IOException;

    protected SearchType getSearchType(org.elasticsearch.action.search.SearchType searchType) {
        if (Objects.isNull(searchType)) {
            return null;
        }
        switch (searchType) {
            case QUERY_THEN_FETCH:
                return SearchType.QueryThenFetch;
            case DFS_QUERY_THEN_FETCH:
                return SearchType.DfsQueryThenFetch;
            default:
                throw new IllegalArgumentException();
        }
    }

    protected List<ExpandWildcard> getExpandWildcard(IndicesOptions.WildcardOptions wildcardOptions) {
        List<ExpandWildcard> expandWildcards = new ArrayList<>(3);
        if (wildcardOptions.matchOpen()) {
            expandWildcards.add(ExpandWildcard.Open);
        }
        if (wildcardOptions.matchClosed()) {
            expandWildcards.add(ExpandWildcard.Closed);
        }
        if (wildcardOptions.includeHidden()) {
            expandWildcards.add(ExpandWildcard.Hidden);
        }

        if (expandWildcards.isEmpty()) {
            expandWildcards.add(ExpandWildcard.None);
        } else if (expandWildcards.size() == 3) {
            expandWildcards.clear();
            expandWildcards.add(ExpandWildcard.All);
        }
        return expandWildcards;
    }

    protected TransportAddress parseAddress(String address) throws UnknownHostException {
        int lastIndexOf = address.lastIndexOf(":");
        return new TransportAddress(InetAddress.getByName(address.substring(0, lastIndexOf)), Integer.parseInt(address.substring(lastIndexOf + 1)));
    }

    protected <T extends JsonpSerializable, R> R parseJson(T response, CheckedFunction<XContentParser, R, IOException> xContentParserCallback) throws IOException {
        String json = toJson(response);
        return parseJson(json, xContentParserCallback);
    }

    protected <R> R parseJson(String json, CheckedFunction<XContentParser, R, IOException> xContentParserCallback) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(this.xContentParserConfiguration, json)) {
            return xContentParserCallback.apply(parser);
        }
    }

    protected <T extends JsonpSerializable> String toJson(T object) {
        String json = "{}";
        if (Objects.nonNull(object)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator generator = this.jsonpMapper.jsonProvider().createGenerator(baos)) {
                object.serialize(generator, this.jsonpMapper);
            }
            try {
                json = baos.toString(StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                logger.warn("could not read json", e);
            }
        }
        return json;
    }

    protected <T> T fromJson(String json, JsonpDeserializer<T> deserializer) throws IOException {
        Objects.requireNonNull(json, "json must not be null");
        Objects.requireNonNull(deserializer, "deserializer must not be null");

        try (Reader reader = new StringReader(json);
             JsonParser parser = jsonpMapper.jsonProvider().createParser(reader)) {
            return deserializer.deserialize(parser, jsonpMapper);
        }
    }
}
