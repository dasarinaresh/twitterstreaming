/**
 * @author NareshDasari
 * @created Aug 10, 2019
 */

package org.naresh.learn.stream.twitter.producers;

import java.util.List;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.stereotype.Component;

@Component
public class PulsarProducer {

	public void publishMessages(List<String> msgs) throws PulsarClientException {
		
		PulsarClient client = PulsarClient.builder()
		        .serviceUrl("pulsar://localhost:6650")
		        .build();
		
		Producer<String> producer = client.newProducer(Schema.STRING)
		        .topic("topicNdasa")
		        .create();
		
		msgs.forEach(msg -> {
			try {
				producer.send(msg);
			} catch (PulsarClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		producer.close();
		client.close();
		
	}
}
