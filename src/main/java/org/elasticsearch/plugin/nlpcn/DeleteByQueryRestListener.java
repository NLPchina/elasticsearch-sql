package org.elasticsearch.plugin.nlpcn;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;


public class DeleteByQueryRestListener extends RestBuilderListener<DeleteByQueryResponse> {

	public DeleteByQueryRestListener(RestChannel channel) {
		super(channel);
	}

	@Override
	public RestResponse buildResponse(DeleteByQueryResponse result, XContentBuilder builder) throws Exception {
		RestStatus restStatus = RestStatus.OK;
		builder.startObject();
		builder.startObject(Fields._INDICES);
		for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : result.getIndices()) {
			builder.startObject(indexDeleteByQueryResponse.getIndex(), XContentBuilder.FieldCaseConversion.NONE);

			builder.startObject(Fields.DELETE);

			builder.field(Fields.DELETED, indexDeleteByQueryResponse.getDeleted());
            builder.field(Fields.FOUND, indexDeleteByQueryResponse.getFound());
			builder.field(Fields.FAILED, indexDeleteByQueryResponse.getFailed());
			builder.field(Fields.MISSING, indexDeleteByQueryResponse.getMissing());


			builder.endObject();

			builder.endObject();
		}
		builder.endObject();
        builder.startObject(Fields._SHARDS);
        ShardOperationFailedException[] failures = result.getShardFailures();
        if (failures != null && failures.length > 0) {
            builder.startArray(Fields.FAILURES);
            for (ShardOperationFailedException shardFailure : failures) {
                builder.startObject();
                builder.field(Fields.INDEX, shardFailure.index());
                builder.field(Fields.SHARD, shardFailure.shardId());
                builder.field(Fields.REASON, shardFailure.reason());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

		builder.endObject();

		return new BytesRestResponse(restStatus, builder);
	}

	static final class Fields {
		static final XContentBuilderString _INDICES = new XContentBuilderString("_indices");
		static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
		static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString DELETED = new XContentBuilderString("deleted");
        static final XContentBuilderString DELETE = new XContentBuilderString("delete");
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
		static final XContentBuilderString MISSING = new XContentBuilderString("missing");
		static final XContentBuilderString FAILED = new XContentBuilderString("failed");
		static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
		static final XContentBuilderString INDEX = new XContentBuilderString("index");
		static final XContentBuilderString SHARD = new XContentBuilderString("shard");
		static final XContentBuilderString REASON = new XContentBuilderString("reason");
	}
}
