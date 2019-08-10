/**
 * @author NareshDasari
 * @created Aug 10, 2019
 */

package org.naresh.learn.stream.twitter.workers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.naresh.learn.stream.twitter.producers.PulsarProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


@Component
public class ProduceTweetWorker {

	private static final String tokenUrl = "https://api.twitter.com/oauth2/token";
	private static final String tweetStatusUrl = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=";
	
	@Autowired
	PulsarProducer pulsarProducer;
	
	String getToken(String key, String secKey) throws ClientProtocolException, IOException, JSONException {
		
		HttpPost request = new HttpPost(tokenUrl);
		String auth = key+":"+secKey;
		byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
		String authHeader = "Basic "+ new String(encodedAuth);
		request.setHeader(HttpHeaders.AUTHORIZATION,authHeader);
		request.setHeader(HttpHeaders.CONTENT_TYPE,"application/x-www-form-urlencoded;charset=UTF-8");
		request.setEntity(new StringEntity("grant_type=client_credentials"));
		HttpClient client = HttpClientBuilder.create().build();
		HttpResponse resp = client.execute(request);
		JSONObject jsonObj =  new JSONObject(EntityUtils.toString(resp.getEntity(), "UTF-8"));
		return jsonObj.getString("access_token");

	}
	
	String getTweets(String user, String token) throws ParseException, ClientProtocolException, JSONException, IOException {
		
		String url = tweetStatusUrl+user+"&count=10";
		HttpGet request = new HttpGet(url);
		request.setHeader(HttpHeaders.AUTHORIZATION,"Bearer "+token);
		HttpClient client = HttpClientBuilder.create().build();
		String resp = EntityUtils.toString(client.execute(request).getEntity(), "UTF-8");
		return resp;
		
	}
	
	public void publishTweetsToKafka(Map<String,String> header, String body) throws ClientProtocolException, IOException, JSONException{
		
		String bearerToken = getToken(header.get("key"), header.get("seckey"));
		JSONArray jsonResp = new JSONArray(getTweets(body, bearerToken));
		
	}
	
	public void publishTweetsToPulsar(Map<String,String> header, String body) throws ClientProtocolException, IOException, JSONException{
		
		String bearerToken = getToken(header.get("key"), header.get("seckey"));
		JSONArray jsonResp = new JSONArray(getTweets(body, bearerToken));
		List<String> msgs = new ArrayList<String>();
		ObjectMapper jsonObjMpper = new ObjectMapper();
		for(int i=0; i< jsonResp.length();i++) {
			
			JsonNode json = jsonObjMpper.readTree(jsonResp.getString(i));
			StringBuilder sb = new StringBuilder();
			sb.append(json.path("user").path("name").asText())
				.append(" Tweeted ")
				.append(json.get("text").asText())
				.append(" on ")
				.append(json.get("created_at").asText());
			
			msgs.add(sb.toString());
		}
		
		pulsarProducer.publishMessages(msgs);
	}
}
