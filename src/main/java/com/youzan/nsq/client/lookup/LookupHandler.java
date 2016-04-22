package com.youzan.nsq.client.lookup;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.bean.NSQNode;

/**
 * 处理lookup命令
 * @author maoxiajun
 *
 */
public class LookupHandler implements ResponseHandler<List<NSQNode>> {

	private static final Logger log = LoggerFactory.getLogger(LookupHandler.class);
	
	@Override
	public List<NSQNode> handleResponse(HttpResponse response) throws ClientProtocolException,
			IOException {
		StatusLine status = response.getStatusLine();
		
		if (status.getStatusCode() != 200) {
			log.error("handle nsqlookup response fail, http status code {}, cause: {}", status.getStatusCode(), status.getReasonPhrase());
			return null;
		}
		
		HttpEntity entity = response.getEntity();
		
		if (null == entity) {
			log.error("nsqlookup response has no content.");
			return null;
		}
		
		ObjectMapper mapper = new ObjectMapper();
		List<NSQNode> nodes = new ArrayList<NSQNode>();
		try (InputStreamReader reader = new InputStreamReader(entity.getContent())) {
			 JsonNode rootNode = mapper.readTree(reader);
			 JsonNode producers = rootNode.path("data").path("producers");
			 Iterator<JsonNode> prodItr = producers.elements();
			 while(prodItr.hasNext()){
				 JsonNode producer = prodItr.next();
				 
				 String host = producer.path("broadcast_address").textValue();
				 int port = producer.path("tcp_port").intValue();
				 
				 // add a new producer into return list
				 nodes.add(new NSQNode(host, port));
			 }
		} catch (JsonParseException e) {
			log.error("error parsing json from lookupd:", e);
		} catch (JsonMappingException e) {
			log.error("error mapping json from lookupd:", e);
		} catch (IOException e) {
			log.error("error reading response from lookupd:", e);
		}
		
		return nodes;
	}

}
