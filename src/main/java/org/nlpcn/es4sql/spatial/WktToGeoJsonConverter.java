package org.nlpcn.es4sql.spatial;

import com.sun.javaws.exceptions.InvalidArgumentException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eliran on 4/8/2015.
 */
public class WktToGeoJsonConverter {
    public static String toGeoJson(String wkt){
        wkt = wkt.toLowerCase();
        //need to check if indexof( !=-1
        int afterType = wkt.indexOf("(");
        if(afterType == -1)
            throw new IllegalArgumentException("not valid wkt");

        String wktType = wkt.substring(0, afterType).trim();
        wkt = wkt.substring(afterType);

        String type="";
        String coordinates="";
        switch (wktType){
            case("point"):
                type = "Point";
                coordinates = pointCoordinatesFromWkt(wkt);
                break;
            case("polygon"):
                type = "Polygon";
                coordinates = polygonCoordinatesFromWkt(wkt);
                break;
            default:
                throw new IllegalArgumentException("not supported wkt type");

        }

        return buildGeoJson(type,coordinates);
    }

    /* currently suppoted this format:
    *  POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))
    *  need to support this format too: POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))
    */
    private static String polygonCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,2);
        String[] points = wkt.split(",");
        List<String> coordinates = new ArrayList<>();
        for(String point : points){
            coordinates.add(extractCoordinateFromPoint(point));
        }
        String joinedCoordinates = String.join(",", coordinates);
        return String.format("[[%s]]", joinedCoordinates);
    }

    private static String buildGeoJson(String type, String coordinates) {
        return String.format("{\"type\":\"%s\", \"coordinates\": %s}", type, coordinates);
    }

    public static String pointCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        return extractCoordinateFromPoint(wkt);
    }

    private static String extractCoordinateFromPoint(String point) {
        String goodPattern = "(\\s*)([0-9\\.]+)(\\s*)([0-9\\.]+)(\\s*)";
        return point.replaceAll(goodPattern,"[$2,$4]");
    }

    private static String removeBrackets(String wkt, int num) {
        String result= wkt;
        for(int i=0;i<num;i++){
            int lastClosingBrackets = result.lastIndexOf(")");
            int firstOpenBrackets = result.indexOf("(");
            if(lastClosingBrackets == -1 || firstOpenBrackets == -1)
               throw new IllegalArgumentException("not enough brackets");
            result = result.substring(firstOpenBrackets+1, lastClosingBrackets);
        }
        return result;
    }

}
