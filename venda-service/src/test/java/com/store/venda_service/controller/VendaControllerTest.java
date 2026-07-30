package com.store.venda_service.controller;

import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Testa se o endpoint responde e publica a venda no topico.
 *
 * @author guilherme.sales
 */
@WebMvcTest(VendaController.class)
class VendaControllerTest {

	@Autowired
	private MockMvc mockMvc;

	// KafkaTemplate falso: o teste nao precisa de um broker no ar
	@MockitoBean
	private KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void devePublicarVendaNoTopico() throws Exception {
		String venda = "{\"id\":\"1\",\"produto\":\"Teclado\"}";

		mockMvc.perform(post("/venda")
				.contentType(MediaType.APPLICATION_JSON)
				.content(venda))
				.andExpect(status().isOk())
				.andExpect(content().string("Venda registrada com sucesso! " + venda));

		// Confirma que a mensagem foi enviada para o topico certo
		verify(kafkaTemplate).send("estoque-topic", venda);
	}
}
