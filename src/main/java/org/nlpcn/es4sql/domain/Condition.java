package org.nlpcn.es4sql.domain;

import java.util.Arrays;

import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * 过滤条件
 * 
 * @author ansj
 */
public class Condition extends Where {

	public static enum OPEAR {
		EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, IS, ISN, IN, NIN , BETWEEN ,NBETWEEN , GEO_INTERSECTS , GEO_BOUNDING_BOX , GEO_DISTANCE , GEO_DISTANCE_RANGE, GEO_POLYGON , GEO_CELL
	};

	private String name;

	private Object value;

	private OPEAR opear;
	
	public Condition(CONN conn, String name, OPEAR oper, Object value) throws SqlParseException {
		super(conn);
		this.opear = null;

		this.name = name;

		this.value = value;
		
		this.opear = oper ;
	}

	public Condition(CONN conn, String name, String oper, Object value) throws SqlParseException {
		super(conn);

		this.opear = null;

		this.name = name;

		this.value = value;

		// EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, IS, ISN, IN, NIN
		switch (oper) {
		case "=":
			this.opear = OPEAR.EQ;
			break;
		case ">":
			this.opear = OPEAR.GT;
			break;
		case "<":
			this.opear = OPEAR.LT;
			break;
		case ">=":
			this.opear = OPEAR.GTE;
			break;
		case "<=":
			this.opear = OPEAR.LTE;
			break;
		case "<>":
			this.opear = OPEAR.N;
			break;
		case "LIKE":
			this.opear = OPEAR.LIKE;
			break;
		case "NOT":
			this.opear = OPEAR.N;
			break;
		case "NOT LIKE":
			this.opear = OPEAR.NLIKE;
			break;
		case "IS":
			this.opear = OPEAR.IS;
			break;
		case "IS NOT":
			this.opear = OPEAR.ISN;
			break;
		case "NOT IN":
			this.opear = OPEAR.NIN;
			break;
		case "IN":
			this.opear = OPEAR.IN;
			break;
		case "BETWEEN":
			this.opear = OPEAR.BETWEEN;
			break;
		case "NOT BETWEEN":
			this.opear = OPEAR.NBETWEEN;
			break;
        case "GEO_INTERSECTS":
            this.opear = OPEAR.GEO_INTERSECTS;
            break;
        case "GEO_BOUNDING_BOX":
            this.opear = OPEAR.GEO_BOUNDING_BOX;
            break;
        case "GEO_DISTANCE":
                this.opear = OPEAR.GEO_DISTANCE;
            break;
        case "GEO_DISTANCE_RANGE":
            this.opear = OPEAR.GEO_DISTANCE_RANGE;
            break;
        case "GEO_POLYGON":
            this.opear = OPEAR.GEO_POLYGON;
            break;
        case "GEO_CELL":
            this.opear = OPEAR.GEO_CELL;
            break;
        default:
			throw new SqlParseException(oper + " is err!");
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public OPEAR getOpear() {
		return opear;
	}

	public void setOpear(OPEAR opear) {
		this.opear = opear;
	}

	@Override
	public String toString() {

		if (value instanceof Object[]) {
			return this.conn + " " + this.name + " " + this.opear + " " + Arrays.toString((Object[]) value);
		} else {
			return this.conn + " " + this.name + " " + this.opear + " " + this.value;
		}
	}

}
