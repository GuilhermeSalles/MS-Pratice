package com.store.venda_service.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuracao do produtor Kafka do venda-service.
 *
 * @author guilherme.sales
 */
@Configuration
public class KafkaProducerConfig {
	// Os ${...} evitam repetir a string; Mantem padronização de codigo e flags
	// Vem do application.properties (localhost na IDE, kafka:29092 no Docker)
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	// Ensina o Kafka a criar produtores: onde fica o broker e como
	// transformar chave e valor em bytes.
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	// E o que voce injeta no service para publicar: kafkaTemplate.send(topico, mensagem)
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
