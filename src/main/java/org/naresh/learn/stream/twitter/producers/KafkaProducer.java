/**
 * @author NareshDasari
 * @created Aug 10, 2019
 */

package org.naresh.learn.stream.twitter.producers;

import java.util.List;
import java.util.Properties;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

	public void publishMessages(List<String> msgs){
		
		
	}
}
