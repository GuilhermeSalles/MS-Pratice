package com.store.bff_service.config;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * As tres saidas do BFF: venda-service, estoque-service e o broker.
 *
 * @author guilherme.sales
 */
@Configuration
public class BffConfig {

	@Bean
	public RestClient restVendaService(@Value("${app.venda-service.url}") String url) {
		return cliente(url);
	}

	@Bean
	public RestClient restEstoqueService(@Value("${app.estoque-service.url}") String url) {
		return cliente(url);
	}

	@Bean(destroyMethod = "close")
	public Admin kafkaAdmin(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
		return Admin.create(Map.of(
				AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
				AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000,
				AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 4000));
	}

	/**
	 * Timeout curto em toda chamada de saida. Sem ele, um servico fora do ar
	 * travaria o painel inteiro em vez de aparecer como OFFLINE.
	 */
	private RestClient cliente(String baseUrl) {
		SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
		factory.setConnectTimeout(Duration.ofSeconds(2));
		factory.setReadTimeout(Duration.ofSeconds(3));
		return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
	}
}
