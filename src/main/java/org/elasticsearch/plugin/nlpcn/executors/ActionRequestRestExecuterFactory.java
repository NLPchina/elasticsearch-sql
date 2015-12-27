package org.elasticsearch.plugin.nlpcn.executors;

/**
 * Created by Eliran on 26/12/2015.
 */
public class ActionRequestRestExecuterFactory {
    public static RestExecutor createExecutor(String format) {
        if(format == null || format.equals("")){
            return new ElasticDefaultRestExecutor();
        }
        if(format.equalsIgnoreCase("csv")){
            return new CSVResultRestExecutor();
        }
        return null;
    }
}
