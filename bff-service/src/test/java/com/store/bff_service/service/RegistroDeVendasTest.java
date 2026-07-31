package com.store.bff_service.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.store.bff_service.client.VendaClient;
import com.store.bff_service.dto.NovaVenda;
import com.store.bff_service.dto.Venda;

/**
 * Testa o contrato do BFF com o venda-service: o id nunca vem da tela.
 *
 * @author guilherme.sales
 */
class RegistroDeVendasTest {

	@Test
	void deveGerarOUuidEPublicarNoVendaService() {
		RestClient.Builder builder = RestClient.builder().baseUrl("http://venda-service:8081");
		MockRestServiceServer vendaService = MockRestServiceServer.bindTo(builder).build();

		vendaService.expect(requestTo("http://venda-service:8081/venda"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(jsonPath("$.nome").value("Bruce Wayne"))
				.andExpect(jsonPath("$.produto").value("Teclado"))
				.andRespond(withSuccess());

		RegistroDeVendas registro = new RegistroDeVendas(new VendaClient(builder.build()));
		Venda venda = registro.registrar(new NovaVenda("Bruce Wayne", "Teclado"));

		// O id e um UUID gerado no BFF, nao um campo do formulario
		assertDoesNotThrow(() -> UUID.fromString(venda.id()));
		assertEquals(1, registro.historico().size());
		vendaService.verify();
	}
}
