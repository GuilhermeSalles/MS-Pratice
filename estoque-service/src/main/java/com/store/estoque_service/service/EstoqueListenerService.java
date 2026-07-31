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

	/** Id do container do listener — e por ele que o painel derruba e sobe o consumidor. */
	public static final String LISTENER_ID = "estoque-listener";

	private final MemoriaEstoque memoria;

	public EstoqueListenerService(MemoriaEstoque memoria) {
		this.memoria = memoria;
	}

	// topics  -> estoque-topic  (vem de app.kafka.topic)
	// groupId -> estoque-group  (vem de spring.kafka.consumer.group-id)
	// Os ${...} evitam repetir a string; Mantem padronização de codigo e flags
	@KafkaListener(id = LISTENER_ID, topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
	public void consumirVenda(String venda) {
		//Aqui vem a lógica para atualizar o estoque, expections etc...
		memoria.registrar(venda);
		System.out.println("### Venda recebida no estoque: " + venda);
	}
}
