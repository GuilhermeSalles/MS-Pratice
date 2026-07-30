package com.store.venda_service.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoint REST que registra uma venda e publica o evento no Kafka.
 *
 * @author guilherme.sales
 */
@RestController
public class VendaController {

	// O bean que o KafkaProducerConfig criou
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	// Nome do topico no application.properties, para nao repetir a string
	// Os ${...} evitam repetir a string; Mantem padronização de codigo e flags
	@Value("${app.kafka.topic}")
	private String topico;

	@PostMapping("/venda")
	public String registrarVenda(@RequestBody String venda) {
		kafkaTemplate.send(topico, venda);
		return "Venda registrada com sucesso! " + venda;
	}
}
