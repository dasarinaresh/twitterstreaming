/**
 * @author NareshDasari
 * @created Aug 10, 2019
 */

package org.naresh.learn.stream.twitter.services;

import java.io.IOException;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.codehaus.jettison.json.JSONException;
import org.naresh.learn.stream.twitter.workers.ProduceTweetWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/produce")
public class GetTweetsService {

	@Autowired
	ProduceTweetWorker produceTweetWorker;
	
	@PostMapping(path = "/kafka")	
	void produceTweetsToKafka(@RequestHeader Map<String,String> header, @RequestBody String body) throws ClientProtocolException, IOException, JSONException {
		produceTweetWorker.publishTweetsToKafka(header, body);
		
	}
	
	@PostMapping(path = "/pulsar")	
	void produceTweetsToPulsar(@RequestHeader Map<String,String> header, @RequestBody String body) throws ClientProtocolException, IOException, JSONException {
		produceTweetWorker.publishTweetsToPulsar(header, body);
		
	}
	
}
