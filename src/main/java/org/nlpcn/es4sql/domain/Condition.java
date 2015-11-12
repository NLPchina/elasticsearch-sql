package org.nlpcn.es4sql.domain;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.elasticsearch.common.collect.ImmutableMap;
import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * 过滤条件
 * 
 * @author ansj
 */
public class Condition extends Where {

	public enum OPEAR {
		EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, IS, ISN, IN, NIN , BETWEEN ,NBETWEEN , GEO_INTERSECTS , GEO_BOUNDING_BOX , GEO_DISTANCE , GEO_DISTANCE_RANGE, GEO_POLYGON , GEO_CELL, IN_TERMS , IDS_QUERY;

        public static Map<String,OPEAR> methodNameToOpear = ImmutableMap.of("in_terms",IN_TERMS,"terms",IN_TERMS,"ids",IDS_QUERY,"ids_query",IDS_QUERY);
        private static BiMap<OPEAR, OPEAR> negatives;


		static {
			negatives = HashBiMap.create(7);
			negatives.put(EQ, N);
			negatives.put(GT, LTE);
			negatives.put(LT, GTE);
			negatives.put(LIKE, NLIKE);
			negatives.put(IS, ISN);
			negatives.put(IN, NIN);
			negatives.put(BETWEEN, NBETWEEN);
		}

		public OPEAR negative() throws SqlParseException {
			OPEAR negative = negatives.get(this);
			negative = negative != null ? negative : negatives.inverse().get(this);
			if (negative == null) {
				throw new SqlParseException("OPEAR negative not supported: " + this);
			}
			return negative;
		}
	}

	private String name;

	private Object value;

	private OPEAR opear;

    private boolean isNested;

	private String nestedPath;

	public Condition(CONN conn, String name, OPEAR oper, Object value,boolean isNested , String nestedPath) throws SqlParseException {
		super(conn);
		this.opear = null;

		this.name = name;

		this.value = value;
		
		this.opear = oper ;

        this.isNested = isNested;

        this.nestedPath = nestedPath;
	}

	public Condition(CONN conn, String name, String oper, Object value,boolean isNested,String nestedPath) throws SqlParseException {
		super(conn);

        this.isNested = isNested;

        this.nestedPath = nestedPath;

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

    public boolean isNested() {
        return isNested;
    }

    public void setNested(boolean isNested) {
        this.isNested = isNested;
    }

    public String getNestedPath() {
        return nestedPath;
    }

    public void setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
    }

    @Override
	public String toString() {
        String result = "";
        if(this.isNested()){
            result = "nested condition ";
            if(this.getNestedPath()!=null){
                result+="on path:" + this.getNestedPath() + " ";
            }
        }
		if (value instanceof Object[]) {
			result += this.conn + " " + this.name + " " + this.opear + " " + Arrays.toString((Object[]) value);
		} else {
			result += this.conn + " " + this.name + " " + this.opear + " " + this.value;
		}
        return result;
	}

    @Override
    public Object clone() throws CloneNotSupportedException {
        try {
            Condition clonedCondition = new Condition(this.getConn(),this.getName(),this.getOpear(),this.getValue(),this.isNested(),this.getNestedPath());
            return clonedCondition;
        } catch (SqlParseException e) {

        }
        return null;
    }
}
