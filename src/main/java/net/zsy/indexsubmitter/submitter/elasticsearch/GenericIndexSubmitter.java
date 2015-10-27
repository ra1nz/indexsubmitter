package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.update.UpdateResponse;

/**
 * 普通的索引提交器 
 * 收到一条数据就提交一条 
 * 同步
 */
public class GenericIndexSubmitter extends AbstractIndexSubmitter {

	public GenericIndexSubmitter(Map<String, String> configurations) {
		super(configurations);
	}

	@Override
	public void submit(String id, String type, Long timestamp, String json) {

		UpdateResponse response = client.prepareUpdate(indexname, type, id).setDoc(json.getBytes())
				.setUpsert(json.getBytes()).get();
		System.out.println(response.isCreated() ? "create "
				: "update " + response.getIndex() + "/" + response.getType() + "/" + response.getId());
	}

}
