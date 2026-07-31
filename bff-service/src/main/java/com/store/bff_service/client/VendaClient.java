package com.store.bff_service.client;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.store.bff_service.dto.Venda;

/**
 * Fala com o venda-service.
 *
 * <p>Chama o mesmo {@code POST /venda} de sempre. O microsservico nao sabe que
 * existe um BFF na frente dele.
 *
 * @author guilherme.sales
 */
@Component
public class VendaClient {

	private final RestClient client;

	public VendaClient(@Qualifier("restVendaService") RestClient client) {
		this.client = client;
	}

	public void publicar(Venda venda) {
		client.post()
				.uri("/venda")
				.contentType(MediaType.APPLICATION_JSON)
				.body(new EventoDeVenda(venda.id(), venda.nome(), venda.produto()))
				.retrieve()
				.toBodilessEntity();
	}

	/** O JSON que vai para o topico — sem o horario, que e coisa de tela. */
	private record EventoDeVenda(String id, String nome, String produto) {
	}
}
