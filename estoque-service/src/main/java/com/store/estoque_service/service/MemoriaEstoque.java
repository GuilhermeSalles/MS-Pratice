package com.store.estoque_service.service;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.springframework.stereotype.Component;

import com.store.estoque_service.model.EventoProcessado;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Guarda em memoria os eventos que o estoque ja processou, para o painel
 * conseguir mostrar o que aconteceu. Substitui o banco de dados que o projeto
 * nao tem: uma fila limitada, sem persistencia.
 *
 * @author guilherme.sales
 */
@Component
public class MemoriaEstoque {

	private static final int LIMITE = 100;
	private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

	private final ObjectMapper mapper = new ObjectMapper();
	private final Deque<EventoProcessado> eventos = new ArrayDeque<>();

	/** Le o JSON da venda e guarda o resultado no topo da lista. */
	public synchronized EventoProcessado registrar(String venda) {
		String id = ler(venda, "id");
		String nome = ler(venda, "nome");
		String produto = ler(venda, "produto");

		EventoProcessado evento = new EventoProcessado(id, nome, produto, LocalTime.now().format(HORA), venda);

		eventos.addFirst(evento);
		while (eventos.size() > LIMITE) {
			eventos.removeLast();
		}
		return evento;
	}

	/** Do mais recente para o mais antigo. */
	public synchronized List<EventoProcessado> listar() {
		return new ArrayList<>(eventos);
	}

	// O payload e uma String crua vinda do topico: se nao for um JSON valido,
	// o campo fica vazio e o evento aparece no painel do mesmo jeito.
	private String ler(String json, String campo) {
		try {
			JsonNode no = mapper.readTree(json).get(campo);
			return no == null || no.isNull() ? "" : no.asText();
		} catch (Exception e) {
			return "";
		}
	}
}
