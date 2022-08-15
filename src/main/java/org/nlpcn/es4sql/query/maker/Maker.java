package org.nlpcn.es4sql.query.maker;

import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryParserFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SpanNearQueryBuilder;
import org.elasticsearch.index.query.SpanQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.Paramer;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.CaseWhenParser;
import org.nlpcn.es4sql.parse.ScriptFilter;
import org.nlpcn.es4sql.parse.SubQueryExpression;
import org.nlpcn.es4sql.spatial.BoundingBoxFilterParams;
import org.nlpcn.es4sql.spatial.DistanceFilterParams;
import org.nlpcn.es4sql.spatial.Point;
import org.nlpcn.es4sql.spatial.PolygonFilterParams;
import org.nlpcn.es4sql.spatial.WktToGeoJsonConverter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class Maker {

	private static final Set<OPEAR> NOT_OPEAR_SET = ImmutableSet.of(OPEAR.N, OPEAR.NIN, OPEAR.ISN, OPEAR.NBETWEEN, OPEAR.NLIKE,OPEAR.NIN_TERMS,OPEAR.NTERM,OPEAR.NREGEXP);

	protected Maker(Boolean isQuery) {

	}

	/**
	 * 构建过滤条件
	 * 
	 * @param cond
	 * @return
	 * @throws SqlParseException
	 */
	protected ToXContent make(Condition cond) throws SqlParseException {

        String name = cond.getName();
        Object value = cond.getValue();

        ToXContent x = null;

        if (value instanceof SQLMethodInvokeExpr) {
            x = make(cond, name, (SQLMethodInvokeExpr) value);
        }
        else if (value instanceof SubQueryExpression){
            x = make(cond,name,((SubQueryExpression)value).getValues());
        } else {
			x = make(cond, name, value);
		}


		return x;
	}

	private ToXContent make(Condition cond, String name, SQLMethodInvokeExpr value) throws SqlParseException {
		ToXContent bqb = null;
		Paramer paramer = null;
		switch (value.getMethodName().toLowerCase()) {
		case "query":
			paramer = Paramer.parseParamer(value);
			QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(paramer.value);
			bqb = Paramer.fullParamer(queryString, paramer);
			bqb = fixNot(cond, bqb);
			break;
		case "matchquery":
		case "match_query":
			paramer = Paramer.parseParamer(value);
			MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(name, paramer.value);
			bqb = Paramer.fullParamer(matchQuery, paramer);
			bqb = fixNot(cond, bqb);
			break;
		case "score":
		case "scorequery":
		case "score_query":
			float boost = Float.parseFloat(value.getParameters().get(1).toString());
			Condition subCond = new Condition(cond.getConn(), cond.getName(),null, cond.getOpear(), value.getParameters().get(0),null);
            bqb = QueryBuilders.constantScoreQuery((QueryBuilder) make(subCond)).boost(boost);
			break;
		case "wildcardquery":
		case "wildcard_query":
			paramer = Paramer.parseParamer(value);
			WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(name, paramer.value);
			bqb = Paramer.fullParamer(wildcardQuery, paramer);
			break;

		case "matchphrasequery":
		case "match_phrase":
		case "matchphrase":
			paramer = Paramer.parseParamer(value);
			MatchPhraseQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(name, paramer.value);
			bqb = Paramer.fullParamer(matchPhraseQuery, paramer);
			break;

        case "multimatchquery":
        case "multi_match":
        case "multimatch":
            paramer = Paramer.parseParamer(value);
            MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(paramer.value);
            bqb = Paramer.fullParamer(multiMatchQuery, paramer);
            break;

        case "spannearquery":
        case "span_near":
        case "spannear":
            paramer = Paramer.parseParamer(value);

            // parse clauses
            List<SpanQueryBuilder> clauses = new ArrayList<>();
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(new NamedXContentRegistry(new SearchModule(Settings.EMPTY, true, Collections.emptyList()).getNamedXContents()), LoggingDeprecationHandler.INSTANCE, paramer.clauses)) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    QueryBuilder query = SpanNearQueryBuilder.parseInnerQueryBuilder(parser);
                    if (!(query instanceof SpanQueryBuilder)) {
                        throw new ParsingException(parser.getTokenLocation(), "spanNear [clauses] must be of type span query");
                    }
                    clauses.add((SpanQueryBuilder) query);
                }
            } catch (IOException e) {
                throw new SqlParseException("could not parse clauses: " + e.getMessage());
            }

            //
            SpanNearQueryBuilder spanNearQuery = QueryBuilders.spanNearQuery(clauses.get(0), Optional.ofNullable(paramer.slop).orElse(SpanNearQueryBuilder.DEFAULT_SLOP));
            for (int i = 1; i < clauses.size(); ++i) {
                spanNearQuery.addClause(clauses.get(i));
            }

            bqb = Paramer.fullParamer(spanNearQuery, paramer);
            break;

        case "matchphraseprefix":
        case "matchphraseprefixquery":
        case "match_phrase_prefix":
            paramer = Paramer.parseParamer(value);
            MatchPhrasePrefixQueryBuilder phrasePrefixQuery = QueryBuilders.matchPhrasePrefixQuery(name, paramer.value);
            bqb = Paramer.fullParamer(phrasePrefixQuery, paramer);
            break;

		default:
			throw new SqlParseException("it did not support this query method " + value.getMethodName());

		}

		return bqb;
	}

	private ToXContent make(Condition cond, String name, Object value) throws SqlParseException {
		ToXContent x = null;
		switch (cond.getOpear()) {
		case ISN:
		case IS:
		case N:
		case EQ:
			if (value == null || value instanceof SQLIdentifierExpr) {
                //todo: change to exists
				if(value == null || ((SQLIdentifierExpr) value).getName().equalsIgnoreCase("missing")) {
                    x = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(name));
				}
				else {
					throw new SqlParseException(String.format("Cannot recoginze Sql identifer %s", ((SQLIdentifierExpr) value).getName()));
				}
				break;
			} else {
				// TODO, maybe use term filter when not analayzed field avalaible to make exact matching?
				// using matchPhrase to achieve equallity.
				// matchPhrase still have some disatvantegs, f.e search for 'word' will match 'some word'
				x = QueryBuilders.matchPhraseQuery(name, value);

				break;
			}
		case LIKE:
        case NLIKE:
			String queryStr = ((String) value);
            queryStr = queryStr.replace('%', '*').replace('_', '?');
            queryStr = queryStr.replace("&PERCENT","%").replace("&UNDERSCORE","_");
			x = QueryBuilders.wildcardQuery(name, queryStr);
			break;
        case REGEXP:
        case NREGEXP:
            Object[] values = (Object[]) value;
            RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(name, values[0].toString());
            if (1 < values.length) {
                String[] flags = values[1].toString().split("\\|");
                RegexpFlag[] regexpFlags = new RegexpFlag[flags.length];
                for (int i = 0; i < flags.length; ++i) {
                    regexpFlags[i] = RegexpFlag.valueOf(flags[i]);
                }
                regexpQuery.flags(regexpFlags);
            }
            if (2 < values.length) {
                regexpQuery.maxDeterminizedStates(Integer.parseInt(values[2].toString()));
            }
            x = regexpQuery;
            break;
		case GT:
            x = QueryBuilders.rangeQuery(name).gt(value);
			break;
		case GTE:
            x = QueryBuilders.rangeQuery(name).gte(value);
			break;
		case LT:
            x = QueryBuilders.rangeQuery(name).lt(value);
			break;
		case LTE:
            x = QueryBuilders.rangeQuery(name).lte(value);
			break;
		case NIN:
		case IN:

		    if (cond.getNameExpr() instanceof SQLCaseExpr) {
                /*
                zhongshu-comment 调用CaseWhenParser解析将Condition的nameExpr属性对象解析为script query
                参考了SqlParser.findSelect()方法，看它是如何解析select中的case when字段的
                 */
                String scriptCode = new CaseWhenParser((SQLCaseExpr) cond.getNameExpr(), null, null).parseCaseWhenInWhere((Object[]) value);
                /*
                zhongshu-comment
                参考DefaultQueryAction.handleScriptField() 将上文得到的scriptCode封装为es的Script对象，
                但又不是完全相同，因为DefaultQueryAction.handleScriptField()是处理select子句中的case when查询，对应es的script_field查询，
                而此处是处理where子句中的case when查询，对应的是es的script query，具体要看官网文档，搜索关键字是"script query"

                搜索结果如下：
                1、文档
                    https://www.elastic.co/guide/en/elasticsearch/reference/6.1/query-dsl-script-query.html
                2、java api
                    https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.1/java-specialized-queries.html
                 */

                x = QueryBuilders.scriptQuery(new Script(scriptCode));

            } else {
                //todo: value is subquery? here or before
                values = (Object[]) value;
                MatchPhraseQueryBuilder[] matchQueries = new MatchPhraseQueryBuilder[values.length];
                for(int i = 0; i < values.length; i++) {
                    matchQueries[i] = QueryBuilders.matchPhraseQuery(name, values[i]);
                }

                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                for(MatchPhraseQueryBuilder matchQuery : matchQueries) {
                    boolQuery.should(matchQuery);
                }
                x = boolQuery;
            }

			break;
		case BETWEEN:
		case NBETWEEN:
            x = QueryBuilders.rangeQuery(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
			break;
        case GEO_INTERSECTS:
            String wkt = cond.getValue().toString();
            try {
                Geometry geometry = getGeometryFromString(wkt);
                x = QueryBuilders.geoIntersectionQuery(cond.getName(), geometry);
            } catch (IOException | ParseException e) {
                e.printStackTrace();
                throw new SqlParseException("couldn't create shapeBuilder from wkt: " + wkt);
            }
            break;
        case GEO_BOUNDING_BOX:
            BoundingBoxFilterParams boxFilterParams = (BoundingBoxFilterParams) cond.getValue();
            Point topLeft = boxFilterParams.getTopLeft();
            Point bottomRight = boxFilterParams.getBottomRight();
            x = QueryBuilders.geoBoundingBoxQuery(cond.getName()).setCorners(topLeft.getLat(), topLeft.getLon(),bottomRight.getLat(), bottomRight.getLon());
            break;
        case GEO_DISTANCE:
            DistanceFilterParams distanceFilterParams = (DistanceFilterParams) cond.getValue();
            Point fromPoint = distanceFilterParams.getFrom();
            String distance = trimApostrophes(distanceFilterParams.getDistance());
            x = QueryBuilders.geoDistanceQuery(cond.getName()).distance(distance).point(fromPoint.getLat(),fromPoint.getLon());
            break;
        case GEO_POLYGON:
            PolygonFilterParams polygonFilterParams = (PolygonFilterParams) cond.getValue();
            ArrayList<GeoPoint> geoPoints = new ArrayList<GeoPoint>();
            for(Point p : polygonFilterParams.getPolygon())
                geoPoints.add(new GeoPoint(p.getLat(), p.getLon()));
            GeoPolygonQueryBuilder polygonFilterBuilder = QueryBuilders.geoPolygonQuery(cond.getName(),geoPoints);
            x = polygonFilterBuilder;
            break;
        case NIN_TERMS:
        case IN_TERMS:
            Object[] termValues = (Object[]) value;
            if(termValues.length == 1 && termValues[0] instanceof SubQueryExpression)
                termValues = ((SubQueryExpression) termValues[0]).getValues();
            Object[] termValuesObjects = new Object[termValues.length];
            for (int i=0;i<termValues.length;i++){
                termValuesObjects[i] = parseTermValue(termValues[i]);
            }
            x = QueryBuilders.termsQuery(name,termValuesObjects);
        break;
        case NTERM:
        case TERM:
            Object term  =( (Object[]) value)[0];
            x = QueryBuilders.termQuery(name, parseTermValue(term));
            break;
        case IDS_QUERY:
            Object[] idsParameters = (Object[]) value;
            String[] ids;
            String type = idsParameters[0].toString();
            if(idsParameters.length ==2 && idsParameters[1] instanceof SubQueryExpression){
                Object[] idsFromSubQuery = ((SubQueryExpression) idsParameters[1]).getValues();
                ids = arrayOfObjectsToStringArray(idsFromSubQuery,0,idsFromSubQuery.length-1);
            }
            else {
                ids =arrayOfObjectsToStringArray(idsParameters,1,idsParameters.length-1);
            }
            x = QueryBuilders.idsQuery(type).addIds(ids);
        break;
        case NNESTED_COMPLEX:
        case NESTED_COMPLEX:
            if(value == null || ! (value instanceof Where) )
                throw new SqlParseException("unsupported nested condition");

            Where whereNested = (Where) value;
            BoolQueryBuilder nestedFilter = QueryMaker.explan(whereNested);

            x = QueryBuilders.nestedQuery(name, nestedFilter, cond.getScoreMode());
        break;
        case CHILDREN_COMPLEX:
            if(value == null || ! (value instanceof Where) )
                throw new SqlParseException("unsupported nested condition");

            Where whereChildren = (Where) value;
            BoolQueryBuilder childrenFilter = QueryMaker.explan(whereChildren);
            //todo: pass score mode
            x = Util.parseQueryBuilder(JoinQueryBuilders.hasChildQuery(name, childrenFilter, ScoreMode.None));

        break;
        case SCRIPT:
            ScriptFilter scriptFilter = (ScriptFilter) value;
            Map<String, Object> params = new HashMap<>();
            if(scriptFilter.containsParameters()){
                params = scriptFilter.getArgs();
            }
            x = QueryBuilders.scriptQuery(new Script(scriptFilter.getScriptType(), Script.DEFAULT_SCRIPT_LANG,scriptFilter.getScript(), params));
        break;
            default:
			throw new SqlParseException("not define type " + cond.getName());
		}

		x = fixNot(cond, x);
		return x;
	}

    private String[] arrayOfObjectsToStringArray(Object[] values, int from, int to) {
        String[] strings = new String[to - from + 1];
        int counter =0;
        for(int i = from ;i<=to;i++){
            strings[counter] = values[i].toString();
            counter++;
        }
        return strings;
    }

    private Geometry getGeometryFromString(String str) throws IOException, ParseException {
        String json;
        if(str.contains("{")) json  = fixJsonFromElastic(str);
        else json = WktToGeoJsonConverter.toGeoJson(trimApostrophes(str));

        return getGeometryFromJson(json);
    }

    /*
    * elastic sends {coordinates=[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]], type=Polygon}
    * proper form is {"coordinates":[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]], "type":"Polygon"}
     *  */
    private String fixJsonFromElastic(String elasticJson) {
        String properJson = elasticJson.replaceAll("=",":");
        properJson = properJson.replaceAll("(type)(:)([a-zA-Z]+)","\"type\":\"$3\"");
        properJson = properJson.replaceAll("coordinates","\"coordinates\"");
        return properJson;
    }

    private Geometry getGeometryFromJson(String json) throws IOException, ParseException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json)) {
            parser.nextToken();
            return GeometryParserFormat.GEOJSON.fromXContent(StandardValidator.instance(true), true, true, parser);
        }
    }

    private String trimApostrophes(String str) {
        return str.substring(1, str.length()-1);
    }

    private ToXContent fixNot(Condition cond, ToXContent bqb) {
		if (NOT_OPEAR_SET.contains(cond.getOpear())) {
				bqb = QueryBuilders.boolQuery().mustNot((QueryBuilder) bqb);
		}
		return bqb;
	}

    private Object parseTermValue(Object termValue) {
        if (termValue instanceof SQLNumericLiteralExpr) {
            termValue = ((SQLNumericLiteralExpr) termValue).getNumber();
            if (termValue instanceof BigDecimal || termValue instanceof Double) {
                termValue = ((Number) termValue).doubleValue();
            } else if (termValue instanceof Float) {
                termValue = ((Number) termValue).floatValue();
            } else if (termValue instanceof BigInteger || termValue instanceof Long) {
                termValue = ((Number) termValue).longValue();
            } else if (termValue instanceof Integer) {
                termValue = ((Number) termValue).intValue();
            } else if (termValue instanceof Short) {
                termValue = ((Number) termValue).shortValue();
            } else if (termValue instanceof Byte) {
                termValue = ((Number) termValue).byteValue();
            }
        } else if (termValue instanceof SQLBooleanExpr) {
            termValue = ((SQLBooleanExpr) termValue).getValue();
        } else {
            termValue = termValue.toString();
        }

        return termValue;
    }
}
