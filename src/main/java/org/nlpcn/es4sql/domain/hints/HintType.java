package org.nlpcn.es4sql.domain.hints;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eliran on 29/8/2015.
 */
public enum HintType
{
    HASH_WITH_TERMS_FILTER,
    JOIN_LIMIT,
    USE_NESTED_LOOPS,
    NL_MULTISEARCH_SIZE,
    USE_SCROLL,
    IGNORE_UNAVAILABLE,
    DOCS_WITH_AGGREGATION,
    ROUTINGS,
    SHARD_SIZE,
    HIGHLIGHT;
}
