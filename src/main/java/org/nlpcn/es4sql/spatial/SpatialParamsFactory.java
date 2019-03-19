package org.nlpcn.es4sql.spatial;


import com.alibaba.druid.sql.ast.SQLExpr;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by Eliran on 1/8/2015.
 */
public class SpatialParamsFactory {
    public static Set<String> allowedMethods ;
    static {
        allowedMethods = new HashSet<>();
        allowedMethods.add("GEO_INTERSECTS");
        allowedMethods.add("GEO_BOUNDING_BOX");
        allowedMethods.add("GEO_DISTANCE");
        allowedMethods.add("GEO_DISTANCE_RANGE");
        allowedMethods.add("GEO_POLYGON");
        allowedMethods.add("GEO_CELL");
    }
    public static boolean isAllowedMethod(String name){
        return allowedMethods.contains(name);
    }
    public static Object generateSpatialParamsObject(String methodName,List<SQLExpr> params){
        switch(methodName){
            case "GEO_INTERSECTS":
                if(params.size() != 2)
                    throw new RuntimeException("GEO_INTERSECTS should have exactly 2 parameters : (fieldName,'WKT') ");
                return params.get(1).toString();
            case "GEO_BOUNDING_BOX":
                if(params.size() != 5)
                    throw new RuntimeException("GEO_BOUNDING_BOX should have exactly 5 parameters : (fieldName,topLeftLon,topLeftLan,bottomRightLon,bottomRightLan) ");
                double topLeftLon = Double.parseDouble(params.get(1).toString());
                double topLeftLat = Double.parseDouble(params.get(2).toString());
                double bottomRightLon = Double.parseDouble(params.get(3).toString());
                double bottomRightLat = Double.parseDouble(params.get(4).toString());
                return new BoundingBoxFilterParams(new Point(topLeftLon,topLeftLat),new Point(bottomRightLon,bottomRightLat));
            case "GEO_DISTANCE":
                if(params.size()!=4)
                    throw new RuntimeException("GEO_DISTANCE should have exactly 4 parameters : (fieldName,distance,fromLon,fromLat) ");
                String distance = params.get(1).toString();
                double lon = Double.parseDouble(params.get(2).toString());
                double lat = Double.parseDouble(params.get(3).toString());
                return new DistanceFilterParams(distance ,new Point(lon,lat));
            case "GEO_DISTANCE_RANGE":
                if(params.size()!=5)
                    throw new RuntimeException("GEO_DISTANCE should have exactly 5 parameters : (fieldName,distanceFrom,distanceTo,fromLon,fromLat) ");
                String distanceFrom = params.get(1).toString();
                String distanceTo = params.get(2).toString();
                lon = Double.parseDouble(params.get(3).toString());
                lat = Double.parseDouble(params.get(4).toString());
                return new RangeDistanceFilterParams(distanceFrom,distanceTo ,new Point(lon,lat));
            case "GEO_POLYGON":
                if(params.size()%2 == 0 || params.size() <= 5)
                    throw new RuntimeException("GEO_POLYGON should have odd num of parameters and > 5 : (fieldName,lon1,lat1,lon2,lat2,lon3,lat3,...) ");
                int numberOfPoints = (params.size()-1)/2;
                List<Point> points = new LinkedList<>();
                for(int i =0 ;i< numberOfPoints ;i ++){
                    int currentPointLocation = 1 + i * 2;
                    lon = Double.parseDouble(params.get(currentPointLocation).toString());
                    lat = Double.parseDouble(params.get(currentPointLocation + 1).toString());
                    points.add(new Point(lon,lat));
                }
                return new PolygonFilterParams(points);
            case "GEO_CELL":
                if(params.size()< 4 || params.size() > 5)
                    throw new RuntimeException("GEO_CELL should have 4 or 5 params (fieldName,lon,lat,precision,neighbors(optional)) ");
                lon = Double.parseDouble(params.get(1).toString());
                lat = Double.parseDouble(params.get(2).toString());
                Point geoHashPoint = new Point(lon,lat);
                int precision = Integer.parseInt(params.get(3).toString());
                if(params.size()==4)
                    return new CellFilterParams(geoHashPoint,precision);
                boolean neighbors = Boolean.parseBoolean(params.get(4).toString());
                return new CellFilterParams(geoHashPoint,precision,neighbors);
            default:
                throw new RuntimeException(String.format("Unknown method name: %s", methodName));
        }
    }
}
