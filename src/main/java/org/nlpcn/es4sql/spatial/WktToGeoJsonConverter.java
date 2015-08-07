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
        int startOfCoordinates = wkt.indexOf("(");
        if(startOfCoordinates == -1)
            throw new IllegalArgumentException("not valid wkt");

        String wktType = wkt.substring(0, startOfCoordinates).trim();
        wkt = wkt.substring(startOfCoordinates);

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


    private static String polygonCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,2);
        String coordinates;
        //if polygon contains inner hole
        boolean polygonContainsInnerHoles = wkt.contains("(");
        if(polygonContainsInnerHoles) {
            String[] polygons = wkt.split("\\s*\\)\\s*,\\s*\\(\\s*");
            String[] coordinatesOfPolygons = new String[polygons.length];
            for (int i = 0; i < polygons.length; i++) {
                String polygonCoordinates = getJsonArrayFromListOfPoints(polygons[i]);
                coordinatesOfPolygons[i] = polygonCoordinates;
            }
            coordinates = String.join(",",coordinatesOfPolygons);
        }
        else {
            coordinates = getJsonArrayFromListOfPoints(wkt);
        }
        return String.format("[%s]", coordinates);
    }

    private static String getJsonArrayFromListOfPoints(String pointsInWkt) {
        String[] points = pointsInWkt.split(",");
        List<String> coordinates = new ArrayList<>();
        for(String point : points){
            coordinates.add(extractCoordinateFromPoint(point));
        }
        String joinedCoordinates = String.join(",", coordinates);
        return String.format("[%s]", joinedCoordinates);
    }

    private static String buildGeoJson(String type, String coordinates) {
        return String.format("{\"type\":\"%s\", \"coordinates\": %s}", type, coordinates);
    }

    public static String pointCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        return extractCoordinateFromPoint(wkt);
    }

    private static String extractCoordinateFromPoint(String point) {
        String pointPattern = "(\\s*)([0-9\\.]+)(\\s*)([0-9\\.]+)(\\s*)";
        return point.replaceAll(pointPattern,"[$2,$4]");
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
