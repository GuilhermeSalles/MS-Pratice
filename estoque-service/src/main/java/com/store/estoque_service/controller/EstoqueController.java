package com.store.estoque_service.controller;

import java.util.List;
import java.util.Map;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.store.estoque_service.model.EventoProcessado;
import com.store.estoque_service.service.EstoqueListenerService;
import com.store.estoque_service.service.MemoriaEstoque;

/**
 * Expoe o que o consumidor ja processou e permite derrubar/subir o listener.
 *
 * <p>Quem consome estes endpoints e o bff-service, para montar o painel. O
 * venda-service continua sem conhecer ninguem: a comunicacao entre os dois
 * microsservicos segue exclusivamente pelo topico.
 *
 * @author guilherme.sales
 */
@RestController
@RequestMapping("/estoque")
public class EstoqueController {

	private final MemoriaEstoque memoria;
	private final KafkaListenerEndpointRegistry registry;

	public EstoqueController(MemoriaEstoque memoria, KafkaListenerEndpointRegistry registry) {
		this.memoria = memoria;
		this.registry = registry;
	}

	/** Eventos que o listener consumiu desde que o servico subiu. */
	@GetMapping("/eventos")
	public List<EventoProcessado> eventos() {
		return memoria.listar();
	}

	/**
	 * Se este endpoint respondeu, o processo esta no ar — por isso
	 * {@code online} e sempre {@code true} aqui. Quem descobre o contrario e o
	 * BFF, quando a chamada nem chega a completar.
	 */
	@GetMapping("/consumidor")
	public Map<String, Object> consumidor() {
		return estado();
	}

	/**
	 * Simula a queda do consumidor sem derrubar o container.
	 *
	 * <p>O {@code stop()} encerra o container do listener: ele sai do consumer
	 * group e para de puxar mensagens. O produtor continua publicando e o lag
	 * do grupo cresce no broker — exatamente o que aconteceria num
	 * {@code docker compose stop estoque-service}.
	 */
	@PostMapping("/consumidor/parar")
	public Map<String, Object> parar() {
		container().stop();
		return estado();
	}

	/**
	 * Sobe o consumidor de volta.
	 *
	 * <p>Ele reentra no grupo, recebe as particoes e retoma do ultimo offset
	 * commitado — consumindo de uma vez tudo que se acumulou.
	 */
	@PostMapping("/consumidor/iniciar")
	public Map<String, Object> iniciar() {
		container().start();
		return estado();
	}

	private Map<String, Object> estado() {
		return Map.of("online", true, "ativo", container().isRunning());
	}

	private MessageListenerContainer container() {
		MessageListenerContainer container = registry.getListenerContainer(EstoqueListenerService.LISTENER_ID);
		if (container == null) {
			throw new IllegalStateException("Listener " + EstoqueListenerService.LISTENER_ID + " nao registrado");
		}
		return container;
	}
}
