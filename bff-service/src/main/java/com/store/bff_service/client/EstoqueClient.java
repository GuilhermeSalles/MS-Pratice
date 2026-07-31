package com.store.bff_service.client;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.store.bff_service.dto.EstadoConsumidor;
import com.store.bff_service.dto.Venda;

/**
 * Fala com o estoque-service.
 *
 * <p>Todo metodo aqui trata a falha como um <b>estado</b>, nao como erro: se o
 * consumidor esta fora do ar, o BFF devolve "offline" em vez de propagar a
 * exception. E o que permite a tela continuar registrando vendas com o estoque
 * derrubado — a falha parcial nao quebra o painel inteiro.
 *
 * @author guilherme.sales
 */
@Component
public class EstoqueClient {

	private static final ParameterizedTypeReference<List<Venda>> LISTA_DE_VENDAS = new ParameterizedTypeReference<>() {
	};

	private final RestClient client;

	public EstoqueClient(@Qualifier("restEstoqueService") RestClient client) {
		this.client = client;
	}

	/** O que o consumidor ja processou, ou lista vazia se ele nao responder. */
	public List<Venda> processadas() {
		try {
			List<Venda> vendas = client.get().uri("/estoque/eventos").retrieve().body(LISTA_DE_VENDAS);
			return vendas == null ? List.of() : vendas;
		} catch (Exception e) {
			return List.of();
		}
	}

	public EstadoConsumidor estado() {
		return chamar("GET", "/estoque/consumidor");
	}

	public EstadoConsumidor parar() {
		return chamar("POST", "/estoque/consumidor/parar");
	}

	public EstadoConsumidor iniciar() {
		return chamar("POST", "/estoque/consumidor/iniciar");
	}

	private EstadoConsumidor chamar(String metodo, String rota) {
		try {
			RestClient.RequestHeadersSpec<?> requisicao = "POST".equals(metodo)
					? client.post().uri(rota)
					: client.get().uri(rota);
			return requisicao.retrieve().body(EstadoConsumidor.class);
		} catch (Exception e) {
			return EstadoConsumidor.offline();
		}
	}
}
