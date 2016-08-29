package org.nlpcn.es4sql.jdbc;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.elasticsearch.search.SearchHit;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class ElasticsearchEnumerator implements Enumerator<Object> {
    private final Iterator<SearchHit> cursor;
    private final Function1<SearchHit, Object> getter;
    private Object current;

    /**
     * Creates an ElasticsearchEnumerator.
     *
     * @param cursor Iterator over Elasticsearch {@link SearchHit} objects
     * @param getter Converts an object into a list of fields
     */
    public ElasticsearchEnumerator(Iterator<SearchHit> cursor, Function1<SearchHit, Object> getter) {
        this.cursor = cursor;
        this.getter = getter;
    }

    public Object current() {
        return current;
    }

    public boolean moveNext() {
        if (cursor.hasNext()) {
            SearchHit map = cursor.next();
            current = getter.apply(map);
            return true;
        } else {
            current = null;
            return false;
        }
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        // nothing to do
    }

    private static Function1<SearchHit, Map> mapGetter() {
        return new Function1<SearchHit, Map>() {
            public Map apply(SearchHit searchHitFields) {
                return (Map) searchHitFields.fields();
            }
        };
    }

    private static Function1<SearchHit, Object> singletonGetter(final String fieldName,
                                                                final Class fieldClass) {
        return new Function1<SearchHit, Object>() {
            public Object apply(SearchHit searchHitFields) {
                if (searchHitFields.fields().isEmpty()) {
                    return convert(searchHitFields.getSource(), fieldClass);
                } else {
                    return convert(searchHitFields.getFields(), fieldClass);
                }
            }
        };
    }

    /**
     * Function that extracts a given set of fields from {@link SearchHit}
     * objects.
     *
     * @param fields List of fields to project
     */
    private static Function1<SearchHit, Object[]> listGetter(
            final List<Map.Entry<String, Class>> fields) {
        return new Function1<SearchHit, Object[]>() {
            public Object[] apply(SearchHit searchHitFields) {
                Object[] objects = new Object[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    final Map.Entry<String, Class> field = fields.get(i);
                    final String name = field.getKey();
                    if (searchHitFields.fields().isEmpty()) {
                        objects[i] = convert(searchHitFields.getSource().get(name), field.getValue());
                    } else {
                        objects[i] = convert(searchHitFields.field(name).getValue(), field.getValue());
                    }
                }
                return objects;
            }
        };
    }

    static Function1<SearchHit, Object> getter(List<Map.Entry<String, Class>> fields) {
        //noinspection unchecked
        return fields == null
                ? (Function1) mapGetter()
                : fields.size() == 1
                ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue())
                : (Function1) listGetter(fields);
    }

    private static Object convert(Object o, Class clazz) {
        if (o == null) {
            return null;
        }
        Primitive primitive = Primitive.of(clazz);
        if (primitive != null) {
            clazz = primitive.boxClass;
        } else {
            primitive = Primitive.ofBox(clazz);
        }
        if (clazz.isInstance(o)) {
            return o;
        }
        if (o instanceof Date && primitive != null) {
            o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
        }
        if (o instanceof Number && primitive != null) {
            return primitive.number((Number) o);
        }
        return o;
    }
}
