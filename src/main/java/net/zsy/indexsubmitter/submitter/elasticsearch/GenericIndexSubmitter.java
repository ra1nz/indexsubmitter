package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

public class GenericIndexSubmitter extends AbstractIndexSubmitter {

	public GenericIndexSubmitter(Map<String, String> configurations) {
		super(configurations);
	}

	@Override
	public void submit(String id, String type, Long timestamp, String json) {
		try {
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexname);
			updateRequest.type(type);
			updateRequest.id(id);

			updateRequest.doc(json.getBytes());

			updateRequest.upsert(json.getBytes());

			UpdateResponse response = client.update(updateRequest).get();
			System.out.println(response.isCreated() ? "create "
					: "update " + response.getIndex() + "/" + response.getType() + "/" + response.getId());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

}
