package org.nlpcn.es4sql;


import org.junit.Test;
import org.nlpcn.es4sql.spatial.WktToGeoJsonConverter;
import org.junit.Assert;

/**
 * Created by Eliran on 4/8/2015.
 */
public class WktToGeoJsonConverterTests {

    @Test
    public void convertPoint_NoRedundantSpaces_ShouldConvert(){
        String wkt = "POINT(12.3 13.3)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [12.3,13.3]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPoint_WithRedundantSpaces_ShouldConvert(){
        String wkt = " POINT ( 12.3 13.3 )   ";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [12.3,13.3]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPoint_RoundNumbers_ShouldConvert(){
        String wkt = "POINT(12 13)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [12,13]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPoint_FirstIsRoundNumber_ShouldConvert(){
        String wkt = "POINT(12 13.3)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [12,13.3]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPoint_SecondIsRoundNumber_ShouldConvert(){
        String wkt = "POINT(12.2 13)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [12.2,13]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPolygon_NoRedundantSpaces_ShouldConvert(){
        String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Polygon\", \"coordinates\": [[[30,10],[40,40],[20,40],[10,20],[30,10]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPolygon_WithRedundantSpaces_ShouldConvert(){
        String wkt = " POLYGON  ( (30  10, 40    40 , 20 40, 10  20, 30 10 ) ) ";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Polygon\", \"coordinates\": [[[30,10],[40,40],[20,40],[10,20],[30,10]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPolygonWithHole_NoRedundantSpaces_ShouldConvert(){
        String wkt = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Polygon\", \"coordinates\": [[[35,10],[45,45],[15,40],[10,20],[35,10]],[[20,30],[35,35],[30,20],[20,30]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertPolygonWithHole_WithRedundantSpaces_ShouldConvert(){
        String wkt = "POLYGON ( (35 10, 45 45, 15 40, 10 20, 35 10 ), (20 30 , 35 35, 30 20,   20 30 ) ) ";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Polygon\", \"coordinates\": [[[35,10],[45,45],[15,40],[10,20],[35,10]],[[20,30],[35,35],[30,20],[20,30]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }


}
