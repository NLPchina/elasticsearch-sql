package org.elasticsearch.plugin.nlpcn;


import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

public class SqlPlug extends Plugin {

	public SqlPlug() {
	}

	@Override
	public String name() {
		return "sql";
	}

	@Override
	public String description() {
		return "Use sql to query elasticsearch.";
	}

	public void onModule(RestModule module)
	{
		module.addRestAction(RestSqlAction.class);
	}
}
