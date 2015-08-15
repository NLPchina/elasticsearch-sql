package org.nlpcn.es4sql.spatial;

/**
 * Created by Eliran on 15/8/2015.
 */
public class CellFilterParams {
    private Point geohashPoint;
    private int precision;
    private boolean neighbors;

    public CellFilterParams(Point geohashPoint, int precision, boolean neighbors) {
        this.geohashPoint = geohashPoint;
        this.precision = precision;
        this.neighbors = neighbors;
    }

    public CellFilterParams(Point geohashPoint, int precision) {
        this(geohashPoint, precision,false);
    }

    public Point getGeohashPoint() {
        return geohashPoint;
    }

    public int getPrecision() {
        return precision;
    }

    public boolean isNeighbors() {
        return neighbors;
    }
}

