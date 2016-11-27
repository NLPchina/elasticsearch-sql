package org.elasticsearch.plugin.nlpcn;


import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.ArrayList;
import java.util.List;

public class SqlPlug implements ActionPlugin {

	public SqlPlug() {
	}


	public String name() {
		return "sql";
	}

	public String description() {
		return "Use sql to query elasticsearch.";
	}


//    @Override
//    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
//        List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> actions = new ArrayList<>(2);
//
//        actions.add(new ActionHandler<>(DidYouMeanAction.INSTANCE, TransportDidYouMeanAction.class));
//        actions.add(new ActionHandler<>(IntentAction.INSTANCE, TransportIntentAction.class));
//
//        return actions;
//    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        List<Class<? extends RestHandler>> restHandlers = new ArrayList<>(1);
        restHandlers.add(RestSqlAction.class);

        return restHandlers;
    }

    //	public void addRestAction(RestModule module)
//	{
//		module.addRestAction(RestSqlAction.class);
//	}
}
