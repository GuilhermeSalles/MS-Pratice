package com.store.estoque_service.service;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.Test;

/**
 * Testa se o listener processa a mensagem recebida do topico.
 *
 * @author guilherme.sales
 */
class EstoqueListenerServiceTest {

	@Test
	void deveConsumirVendaRecebida() {
		String venda = "{\"id\":\"1\",\"produto\":\"Teclado\"}";

		// Captura o que a aplicacao escreve no console
		PrintStream consoleOriginal = System.out;
		ByteArrayOutputStream saida = new ByteArrayOutputStream();
		System.setOut(new PrintStream(saida));

		try {
			new EstoqueListenerService().consumirVenda(venda);
		} finally {
			System.setOut(consoleOriginal);
		}

		assertTrue(saida.toString().contains("### Venda recebida no estoque: " + venda));
	}
}
