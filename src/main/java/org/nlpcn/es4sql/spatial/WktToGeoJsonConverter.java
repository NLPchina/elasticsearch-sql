package org.nlpcn.es4sql.spatial;


import com.google.common.base.Joiner;

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
            case("linestring"):
                type = "LineString";
                coordinates = lineStringCoordinatesFromWkt(wkt);
                break;
            case("multipolygon"):
                type = "MultiPolygon";
                coordinates  = multiPolygonCoordinatesFromWkt(wkt);
                break;
            case("multipoint"):
                type = "MultiPoint";
                coordinates = multiPointCoordinatesFromWkt(wkt);
                break;
            case("multilinestring"):
                type = "MultiLineString";
                coordinates = multiLineStringCoordinatesFromWkt(wkt);
                break;
            default:
                throw new IllegalArgumentException("not supported wkt type");

        }

        return buildGeoJson(type,coordinates);
    }
    //input: ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))
    private static String multiLineStringCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        String lineStringsWithPipeSeparator = wkt.replaceAll("\\s*\\)\\s*,\\s*\\(",")|(");
        String[] lineStrings = lineStringsWithPipeSeparator.split("\\|");
        String[] coordinates = new String[lineStrings.length];
        for (int i=0;i<lineStrings.length;i++){
            coordinates[i] = lineStringCoordinatesFromWkt(lineStrings[i]);
        }
        String multiLineStringCoordinates = Joiner.on(",").join(coordinates);
        return String.format("[%s]", multiLineStringCoordinates);

    }

    //input v1:MULTIPOINT (10 40, 40 30, 20 20, 30 10)
    //v2:MULTIPOINT ((10 40), (40 30), (20 20), (30 10))
    private static String multiPointCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        boolean isSecondVersionMultiPoint = wkt.contains("(");
        String coordinates = "";
        if(isSecondVersionMultiPoint){
            //(10 40), (40 30), (20 20)-> 10 40, 40 30, 20 20
            wkt = wkt.replaceAll("\\(|\\)" ,"");
        }
        coordinates = getJsonArrayFromListOfPoints(wkt);
        return coordinates;
    }

    //input (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))
    private static String multiPolygonCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        String polygonsWithPipeSeparator = wkt.replaceAll("\\s*\\)\\s*\\)\\s*,\\s*\\(\\s*\\(\\s*","))|((");
        String[] polygons = polygonsWithPipeSeparator.split("\\|");
        String[] polygonsCoordinates = new String[polygons.length];
        for (int i=0;i<polygons.length;i++){
            polygonsCoordinates[i] = polygonCoordinatesFromWkt(polygons[i]);
        }
        String coordinates = Joiner.on(",").join(polygonsCoordinates);
        return String.format("[%s]", coordinates);
    }

    //input : (30 10, 10 30, 40 40)
    private static String lineStringCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        return getJsonArrayFromListOfPoints(wkt);
    }

    //input: v1:((35 10, 45 45, 15 40, 10 20, 35 10))
    //v2:((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))
    private static String polygonCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,2);
        String coordinates;
        boolean polygonContainsInnerHoles = wkt.contains("(");
        if(polygonContainsInnerHoles) {
            String[] polygons = wkt.split("\\s*\\)\\s*,\\s*\\(\\s*");
            String[] coordinatesOfPolygons = new String[polygons.length];
            for (int i = 0; i < polygons.length; i++) {
                String polygonCoordinates = getJsonArrayFromListOfPoints(polygons[i]);
                coordinatesOfPolygons[i] = polygonCoordinates;
            }
            coordinates = Joiner.on(",").join(coordinatesOfPolygons);
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

        String joinedCoordinates = Joiner.on(",").join(coordinates);
        return String.format("[%s]", joinedCoordinates);
    }

    private static String buildGeoJson(String type, String coordinates) {
        return String.format("{\"type\":\"%s\", \"coordinates\": %s}", type, coordinates);
    }
    //input : (30 10)
    public static String pointCoordinatesFromWkt(String wkt) {
        wkt = removeBrackets(wkt,1);
        return extractCoordinateFromPoint(wkt);
    }

    private static String extractCoordinateFromPoint(String point) {
        String pointPattern = "(\\s*)([0-9\\.-]+)(\\s*)([0-9\\.-]+)(\\s*)";
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
