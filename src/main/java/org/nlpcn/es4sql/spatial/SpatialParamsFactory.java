package org.nlpcn.es4sql.spatial;

import org.durid.sql.ast.SQLExpr;
import org.durid.sql.parser.SQLParseException;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.util.List;

/**
 * Created by Eliran on 1/8/2015.
 */
public class SpatialParamsFactory {
    public static Object generateSpatialParamsObject(String methodName,List<SQLExpr> params){
        switch(methodName){
            case "GEO_INTERSECTS":
                if(params.size() != 2)
                    throw new SQLParseException("GEO_INTERSECTS should have exactly 2 parameters : (fieldName,'WKT') ");
                return params.get(1).toString();
            case "GEO_BOUNDING_BOX":
                if(params.size() != 5)
                    throw new SQLParseException("GEO_BOUNDING_BOX should have exactly 5 parameters : (fieldName,topLeftLon,topLeftLan,bottomRightLon,bottomRightLan) ");
                double topLeftLon = Double.parseDouble(params.get(1).toString());
                double topLeftLat = Double.parseDouble(params.get(2).toString());
                double bottomRightLon = Double.parseDouble(params.get(3).toString());
                double bottomRightLat = Double.parseDouble(params.get(4).toString());
                return new BoundingBoxFilterParams(new Point(topLeftLon,topLeftLat),new Point(bottomRightLon,bottomRightLat));
            default:
                throw new SQLParseException(String.format("Unknown method name: %s", methodName));
        }
    }
}
