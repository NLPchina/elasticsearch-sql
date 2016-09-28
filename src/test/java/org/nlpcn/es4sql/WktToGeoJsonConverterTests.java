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
    public void convertPoint_NegativeCoordinates_ShouldConvert(){
        String wkt = "POINT(-12.2 13)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Point\", \"coordinates\": [-12.2,13]}";
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
    public void convertPolygon_NegativeCoordinates_ShouldConvert(){
        String wkt = "POLYGON ((-30 10, 40 40, 20 40, 10 20, -30 10))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"Polygon\", \"coordinates\": [[[-30,10],[40,40],[20,40],[10,20],[-30,10]]]}";
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

    @Test
    public void convertLineString_NoRedundantSpaces_ShouldConvert(){
        String wkt = "LINESTRING (30 10, 10 30, 40 40)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"LineString\", \"coordinates\": [[30,10],[10,30],[40,40]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertLineString_NegativeCoordinates_ShouldConvert(){
        String wkt = "LINESTRING (-30 10, 10 30, 40 40)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"LineString\", \"coordinates\": [[-30,10],[10,30],[40,40]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertLineString_WithRedundantSpaces_ShouldConvert(){
        String wkt = "LINESTRING (     30  10, 10 30 , 40 40    )";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"LineString\", \"coordinates\": [[30,10],[10,30],[40,40]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertMultiPolygon_NoRedundantSpaces_ShouldConvert(){
        String wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiPolygon\", \"coordinates\": [[[[30,20],[45,40],[10,40],[30,20]]],[[[15,5],[40,10],[10,20],[5,10],[15,5]]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }
    @Test
    public void convertMultiPolygon_WithRedundantSpaces_ShouldConvert(){
        String wkt = "MULTIPOLYGON ( ((30 20, 45 40, 10 40, 30 20) ) , ((15 5, 40 10, 10 20, 5 10, 15 5)))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiPolygon\", \"coordinates\": [[[[30,20],[45,40],[10,40],[30,20]]],[[[15,5],[40,10],[10,20],[5,10],[15,5]]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }
    @Test
    public void convertMultiPolygon_OnePolygonHaveHoles_ShouldConvert(){
        String wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20),(20 30, 35 35, 30 20, 20 30)),((15 5, 40 10, 10 20, 5 10, 15 5)))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiPolygon\", \"coordinates\": [[[[30,20],[45,40],[10,40],[30,20]],[[20,30],[35,35],[30,20],[20,30]]],[[[15,5],[40,10],[10,20],[5,10],[15,5]]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertMultiPoint_V1_ShouldConvert(){
        String wkt = "MULTIPOINT (10 40, 40 30, 20 20, 30 10)";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiPoint\", \"coordinates\": [[10,40],[40,30],[20,20],[30,10]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertMultiPoint_V2_ShouldConvert(){
        String wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiPoint\", \"coordinates\": [[10,40],[40,30],[20,20],[30,10]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }

    @Test
    public void convertMultiLineString_NoRedundantSpaces_ShouldConvert(){
        String wkt = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiLineString\", \"coordinates\": [[[10,10],[20,20],[10,40]],[[40,40],[30,30],[40,20],[30,10]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }
    @Test
    public void convertMultiLineString_WithRedundantSpaces_ShouldConvert(){
        String wkt = "MULTILINESTRING ( (10 10, 20 20, 10   40 ) , (40 40, 30 30, 40 20, 30 10))";
        String geoJson = WktToGeoJsonConverter.toGeoJson(wkt);
        String expectedGeoJson = "{\"type\":\"MultiLineString\", \"coordinates\": [[[10,10],[20,20],[10,40]],[[40,40],[30,30],[40,20],[30,10]]]}";
        Assert.assertEquals(expectedGeoJson,geoJson);
    }
}
