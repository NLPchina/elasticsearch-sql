package org.nlpcn.es4sql.domain;

import org.nlpcn.es4sql.exception.SqlParseException;

/**
 * Created by Eliran on 29/8/2015.
 */
public enum Hint {
    HASH_WITH_TERMS_FILTER;

    public static Hint hintFromString(String hint){
        switch (hint) {
            case "! HASH_WITH_TERMS_FILTER":
                return HASH_WITH_TERMS_FILTER;
            default:
                return null;
        }
    }
}
