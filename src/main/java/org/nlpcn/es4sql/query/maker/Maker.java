package org.nlpcn.es4sql.query.maker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.nlpcn.es4sql.Util;
import org.nlpcn.es4sql.domain.Condition;
import org.nlpcn.es4sql.domain.Condition.OPEAR;
import org.nlpcn.es4sql.domain.Paramer;
import org.nlpcn.es4sql.domain.Query;
import org.nlpcn.es4sql.domain.Where;
import org.nlpcn.es4sql.exception.SqlParseException;


import org.nlpcn.es4sql.parse.ScriptFilter;
import org.nlpcn.es4sql.parse.SubQueryExpression;
import org.nlpcn.es4sql.spatial.*;

public abstract class Maker {


	private static final Set<OPEAR> NOT_OPEAR_SET = ImmutableSet.of(OPEAR.N, OPEAR.NIN, OPEAR.ISN, OPEAR.NBETWEEN, OPEAR.NLIKE);



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
			Float boost = Float.parseFloat(value.getParameters().get(1).toString());
			Condition subCond = new Condition(cond.getConn(), cond.getName(), cond.getOpear(), value.getParameters().get(0));
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
			MatchQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(name, paramer.value);
			bqb = Paramer.fullParamer(matchPhraseQuery, paramer);
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
				if(value == null || ((SQLIdentifierExpr) value).getName().equalsIgnoreCase("missing")) {
                    x = QueryBuilders.missingQuery(name);
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
			String queryStr = ((String) value).replace('%', '*').replace('_', '?');
			x = QueryBuilders.wildcardQuery(name, queryStr);
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
            //todo: value is subquery? here or before
			Object[] values = (Object[]) value;
			MatchQueryBuilder[] matchQueries = new MatchQueryBuilder[values.length];
			for(int i = 0; i < values.length; i++) {
				matchQueries[i] = QueryBuilders.matchPhraseQuery(name, values[i]);
			}

            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for(MatchQueryBuilder matchQuery : matchQueries) {
                boolQuery.should(matchQuery);
            }
            x = boolQuery;
			break;
		case BETWEEN:
		case NBETWEEN:
            x = QueryBuilders.rangeQuery(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
			break;
        case GEO_INTERSECTS:
            String wkt = cond.getValue().toString();
            try {
                ShapeBuilder shapeBuilder = getShapeBuilderFromString(wkt);
                x = QueryBuilders.geoShapeQuery(cond.getName(), shapeBuilder,ShapeRelation.INTERSECTS);
            } catch (IOException e) {
                e.printStackTrace();
                throw new SqlParseException("couldn't create shapeBuilder from wkt: " + wkt);
            }
            break;
        case GEO_BOUNDING_BOX:
            BoundingBoxFilterParams boxFilterParams = (BoundingBoxFilterParams) cond.getValue();
            Point topLeft = boxFilterParams.getTopLeft();
            Point bottomRight = boxFilterParams.getBottomRight();
            x = QueryBuilders.geoBoundingBoxQuery(cond.getName()).topLeft(topLeft.getLat(), topLeft.getLon()).bottomRight(bottomRight.getLat(), bottomRight.getLon());
            break;
        case GEO_DISTANCE:
            DistanceFilterParams distanceFilterParams = (DistanceFilterParams) cond.getValue();
            Point fromPoint = distanceFilterParams.getFrom();
            String distance = trimApostrophes(distanceFilterParams.getDistance());
            x = QueryBuilders.geoDistanceQuery(cond.getName()).distance(distance).lon(fromPoint.getLon()).lat(fromPoint.getLat());
            break;
        case GEO_DISTANCE_RANGE:
            RangeDistanceFilterParams rangeDistanceFilterParams = (RangeDistanceFilterParams) cond.getValue();
            fromPoint = rangeDistanceFilterParams.getFrom();
            String distanceFrom = trimApostrophes(rangeDistanceFilterParams.getDistanceFrom());
            String distanceTo = trimApostrophes(rangeDistanceFilterParams.getDistanceTo());
            x = QueryBuilders.geoDistanceRangeQuery(cond.getName()).from(distanceFrom).to(distanceTo).lon(fromPoint.getLon()).lat(fromPoint.getLat());
            break;
        case GEO_POLYGON:
            PolygonFilterParams polygonFilterParams = (PolygonFilterParams) cond.getValue();
            GeoPolygonQueryBuilder polygonFilterBuilder = QueryBuilders.geoPolygonQuery(cond.getName());
            for(Point p : polygonFilterParams.getPolygon())
                polygonFilterBuilder.addPoint(p.getLat(),p.getLon());
            x = polygonFilterBuilder;
            break;
        case GEO_CELL:
            CellFilterParams cellFilterParams = (CellFilterParams) cond.getValue();
            Point geoHashPoint = cellFilterParams.getGeohashPoint();
            x = QueryBuilders.geoHashCellQuery(cond.getName()).point(geoHashPoint.getLat(),geoHashPoint.getLon()).precision(cellFilterParams.getPrecision()).neighbors(cellFilterParams.isNeighbors());
            break;
        case IN_TERMS:
            Object[] termValues = (Object[]) value;
            if(termValues.length == 1 && termValues[0] instanceof SubQueryExpression)
                termValues = ((SubQueryExpression) termValues[0]).getValues();
            x = QueryBuilders.termsQuery(name,termValues);
        break;
        case TERM:
            Object term  =( (Object[]) value)[0];
            x = QueryBuilders.termQuery(name,term);
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
        case NESTED_COMPLEX:
            if(value == null || ! (value instanceof Where) )
                throw new SqlParseException("unsupported nested condition");
            Where where = (Where) value;
            BoolQueryBuilder nestedFilter = QueryMaker.explan(where);

            x = QueryBuilders.nestedQuery(name,nestedFilter);

        break;
        case SCRIPT:
            ScriptFilter scriptFilter = (ScriptFilter) value;
            Map<String, Object> params = null;
            if(scriptFilter.containsParameters()){
                params = scriptFilter.getArgs();
            }
            x = QueryBuilders.scriptQuery(new Script(scriptFilter.getScript(), ScriptService.ScriptType.INLINE, null, params));
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

    private ShapeBuilder getShapeBuilderFromString(String str) throws IOException {
        String json;
        if(str.contains("{")) json  = fixJsonFromElastic(str);
        else json = WktToGeoJsonConverter.toGeoJson(trimApostrophes(str));

        return getShapeBuilderFromJson(json);
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

    private ShapeBuilder getShapeBuilderFromJson(String json) throws IOException {
        XContentParser parser = null;
        parser = JsonXContent.jsonXContent.createParser(json);
        parser.nextToken();
        return ShapeBuilder.parse(parser);
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

}
