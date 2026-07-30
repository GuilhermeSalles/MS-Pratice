package com.store.estoque_service.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Escuta os eventos de venda publicados no Kafka e atualiza o estoque.
 *
 * @author guilherme.sales
 */
@Service
public class EstoqueListenerService {

	// topics  -> estoque-topic  (vem de app.kafka.topic)
	// groupId -> estoque-group  (vem de spring.kafka.consumer.group-id)
	// Os ${...} evitam repetir a string; Mantem padronização de codigo e flags
	@KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
	public void consumirVenda(String venda) {
		//Aqui vem a lógica para atualizar o estoque, expections etc...
		System.out.println("### Venda recebida no estoque: " + venda);
	}
}
