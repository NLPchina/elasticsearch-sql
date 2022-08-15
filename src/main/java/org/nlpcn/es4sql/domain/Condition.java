package org.nlpcn.es4sql.domain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.lucene.search.join.ScoreMode;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.ChildrenType;
import org.nlpcn.es4sql.parse.NestedType;

/**
 * 过滤条件
 *
 * @author ansj
 */
public class Condition extends Where {

    public enum OPEAR {
        EQ, GT, LT, GTE, LTE, N, LIKE, NLIKE, REGEXP, NREGEXP, IS, ISN, IN, NIN, BETWEEN, NBETWEEN, GEO_INTERSECTS, GEO_BOUNDING_BOX, GEO_DISTANCE, GEO_POLYGON, IN_TERMS, TERM, IDS_QUERY, NESTED_COMPLEX, NNESTED_COMPLEX, CHILDREN_COMPLEX, SCRIPT,NIN_TERMS,NTERM;

        public static Map<String, OPEAR> methodNameToOpear;

        private static BiMap<OPEAR, OPEAR> negatives;

        static {
            methodNameToOpear = new HashMap<>();
            methodNameToOpear.put("term", TERM);
            methodNameToOpear.put("matchterm", TERM);
            methodNameToOpear.put("match_term", TERM);
            methodNameToOpear.put("terms", IN_TERMS);
            methodNameToOpear.put("in_terms", IN_TERMS);
            methodNameToOpear.put("ids", IDS_QUERY);
            methodNameToOpear.put("ids_query", IDS_QUERY);
            methodNameToOpear.put("regexp", REGEXP);
            methodNameToOpear.put("regexp_query", REGEXP);
        }

        static {
            negatives = HashBiMap.create(7);
            negatives.put(EQ, N);
            negatives.put(IN_TERMS, NIN_TERMS);
			negatives.put(TERM, NTERM);
            negatives.put(GT, LTE);
            negatives.put(LT, GTE);
            negatives.put(LIKE, NLIKE);
            negatives.put(IS, ISN);
            negatives.put(IN, NIN);
            negatives.put(BETWEEN, NBETWEEN);
            negatives.put(REGEXP, NREGEXP);
            negatives.put(NESTED_COMPLEX, NNESTED_COMPLEX);
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

    private SQLExpr nameExpr;

    private Object value;

    public SQLExpr getNameExpr() {
        return nameExpr;
    }

    public SQLExpr getValueExpr() {
        return valueExpr;
    }

    private SQLExpr valueExpr;

    private OPEAR opear;

    private Object relationshipType;

    private boolean isNested;
    private String nestedPath;
    private String innerHits;
    private ScoreMode scoreMode;

    private boolean isChildren;
    private String childType;

    public Condition(CONN conn) {
        super(conn);
    }

    public Condition(CONN conn, String field, SQLExpr nameExpr, String condition, Object obj, SQLExpr valueExpr) throws SqlParseException {
        this(conn, field, nameExpr, condition, obj, valueExpr, null);
    }

    public Condition(CONN conn, String field, SQLExpr nameExpr, OPEAR condition, Object obj, SQLExpr valueExpr) throws SqlParseException {
        this(conn, field, nameExpr, condition, obj, valueExpr, null);
    }

    public Condition(CONN conn, String name, SQLExpr nameExpr, String oper, Object value, SQLExpr valueExpr, Object relationshipType) throws
            SqlParseException {
        super(conn);

        this.opear = null;
        this.name = name;
        this.value = value;
        this.nameExpr = nameExpr;
        this.valueExpr = valueExpr;

        this.relationshipType = relationshipType;

        if (this.relationshipType != null) {
            if (this.relationshipType instanceof NestedType) {
                NestedType nestedType = (NestedType) relationshipType;

                this.isNested = true;
                this.nestedPath = nestedType.path;
                this.innerHits = nestedType.getInnerHits();
                this.scoreMode = nestedType.getScoreMode();
                this.isChildren = false;
                this.childType = "";
            } else if (relationshipType instanceof ChildrenType) {
                ChildrenType childrenType = (ChildrenType) relationshipType;

                this.isNested = false;
                this.nestedPath = "";
                this.isChildren = true;
                this.childType = childrenType.childType;
            }
        } else {
            this.isNested = false;
            this.nestedPath = "";
            this.isChildren = false;
            this.childType = "";
        }

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
            case "GEO_POLYGON":
                this.opear = OPEAR.GEO_POLYGON;
                break;
            case "NESTED":
                this.opear = OPEAR.NESTED_COMPLEX;
                break;
            case "NOT NESTED":
                this.opear = OPEAR.NNESTED_COMPLEX;
                break;
            case "CHILDREN":
                this.opear = OPEAR.CHILDREN_COMPLEX;
                break;
            case "SCRIPT":
                this.opear = OPEAR.SCRIPT;
                break;
            default:
                throw new SqlParseException(oper + " is err!");
        }
    }


    public Condition(CONN conn,
                     String name,
                     SQLExpr nameExpr,
                     OPEAR oper,
                     Object value,
                     SQLExpr valueExpr,
                     Object relationshipType
    ) throws SqlParseException {
        super(conn);

        this.opear = null;
        this.nameExpr = nameExpr;
        this.valueExpr = valueExpr;
        this.name = name;
        this.value = value;
        this.opear = oper;
        this.relationshipType = relationshipType;

        if (this.relationshipType != null) {
            if (this.relationshipType instanceof NestedType) {
                NestedType nestedType = (NestedType) relationshipType;

                this.isNested = true;
                this.nestedPath = nestedType.path;
                this.innerHits = nestedType.getInnerHits();
                this.scoreMode = nestedType.getScoreMode();
                this.isChildren = false;
                this.childType = "";
            } else if (relationshipType instanceof ChildrenType) {
                ChildrenType childrenType = (ChildrenType) relationshipType;

                this.isNested = false;
                this.nestedPath = "";
                this.isChildren = true;
                this.childType = childrenType.childType;
            }
        } else {
            this.isNested = false;
            this.nestedPath = "";
            this.isChildren = false;
            this.childType = "";
        }
    }

    public String getOpertatorSymbol() throws SqlParseException {
        switch (opear) {
            case EQ:
                return "==";
            case GT:
                return ">";
            case LT:
                return "<";
            case GTE:
                return ">=";
            case LTE:
                return "<=";
            case N:
                return "<>";
            case IS:
                return "==";

            case ISN:
                return "!=";
            default:
                throw new SqlParseException(opear + " is err!");
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

    public Object getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(Object relationshipType) {
        this.relationshipType = relationshipType;
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

    public String getInnerHits() {
        return innerHits;
    }

    public void setInnerHits(String innerHits) {
        this.innerHits = innerHits;
    }

    public ScoreMode getScoreMode() {
        return scoreMode;
    }

    public boolean isChildren() {
        return isChildren;
    }

    public void setChildren(boolean isChildren) {
        this.isChildren = isChildren;
    }

    public String getChildType() {
        return childType;
    }

    public void setChildType(String childType) {
        this.childType = childType;
    }

    @Override
    public String toString() {
        String result = "";

        if (this.isNested()) {
            result = "nested condition ";
            if (this.getNestedPath() != null) {
                result += "on path:" + this.getNestedPath() + " ";
            }

            if (this.getInnerHits() != null) {
                result += "inner_hits:" + this.getInnerHits() + " ";
            }
            if (this.getScoreMode() != null) {
                result += "score_mode:" + this.getScoreMode() + " ";
            }
        } else if (this.isChildren()) {
            result = "children condition ";

            if (this.getChildType() != null) {
                result += "on child: " + this.getChildType() + " ";
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
            Condition clonedCondition = new Condition(this.getConn(), this.getName(),this.getNameExpr(), this.getOpear(), this.getValue(),this.getValueExpr(), this.getRelationshipType());
            return clonedCondition;
        } catch (SqlParseException e) {

        }
        return null;
    }
}
