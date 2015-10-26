package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GenericIndexSubmitter extends AbstractIndexSubmitter {

	private ObjectMapper objectMapper = new ObjectMapper();

	public GenericIndexSubmitter(Map<String, String> configurations) {
		super(configurations);
	}

	@Override
	public void submit(String message) {
		try {
			Map<?, ?> map = objectMapper.readValue(message.getBytes(), Map.class);
			String json = map.get(DATA).toString();
			String type = map.get(TYPE).toString();
			String id = map.get(ID).toString();

			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexname);
			updateRequest.type(types.get(type));
			updateRequest.id(id);

			updateRequest.doc(json.getBytes());

			updateRequest.upsert(json.getBytes());

			UpdateResponse response = client.update(updateRequest).get();
			System.out.println(response.isCreated() ? "create " : "update " + indexname + "/" + type);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

}
