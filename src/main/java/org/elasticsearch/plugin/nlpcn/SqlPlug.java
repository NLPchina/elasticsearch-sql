package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class SqlPlug extends AbstractPlugin {

	public SqlPlug() {
	}

	@Override
	public String name() {
		return "sql";
	}

	@Override
	public String description() {
		return "sql query by sql.";
	}

	public void onModule(RestModule module) {
		module.addRestAction(RestSqlAction.class);
	}
}
