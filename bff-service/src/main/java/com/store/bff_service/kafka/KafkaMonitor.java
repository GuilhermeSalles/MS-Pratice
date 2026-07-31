package com.store.bff_service.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.store.bff_service.dto.LeituraDoBroker;

/**
 * Le offsets e lag direto do Kafka, com o AdminClient.
 *
 * <p>O BFF <b>nao produz nem consome</b> eventos — publicar e do venda-service,
 * consumir e do estoque-service. Aqui e so leitura, para a tela ter numeros de
 * verdade em vez de uma contagem da aplicacao.
 *
 * @author guilherme.sales
 */
@Component
public class KafkaMonitor {

	private final Admin admin;
	private final String topico;
	private final String grupo;
	// O topico do projeto tem uma particao so, entao basta olhar a de numero 0
	private final TopicPartition particao;

	public KafkaMonitor(Admin admin, @Value("${app.kafka.topic}") String topico,
			@Value("${app.kafka.group}") String grupo) {
		this.admin = admin;
		this.topico = topico;
		this.grupo = grupo;
		this.particao = new TopicPartition(topico, 0);
	}

	public LeituraDoBroker ler() {
		try {
			long gravados = gravados();
			long lidos = lidos();
			return new LeituraDoBroker(topico, grupo, gravados, lidos, gravados - lidos, consumidores(), true);
		} catch (Exception e) {
			// Antes da primeira venda o topico ainda nem existe. Isso e log
			// vazio, nao broker fora do ar.
			boolean topicoAindaNaoExiste = e.getCause() instanceof UnknownTopicOrPartitionException;
			return new LeituraDoBroker(topico, grupo, 0, 0, 0, 0, topicoAindaNaoExiste);
		}
	}

	/** Onde o log termina: total de eventos ja gravados no topico. */
	private long gravados() throws Exception {
		return admin.listOffsets(Map.of(particao, OffsetSpec.latest())).all().get().get(particao).offset();
	}

	/** Ate onde o consumer group confirmou leitura. */
	private long lidos() throws Exception {
		OffsetAndMetadata commitado = admin.listConsumerGroupOffsets(grupo)
				.partitionsToOffsetAndMetadata().get().get(particao);
		return commitado == null ? 0 : commitado.offset();
	}

	/** Cai para zero quando o consumidor sai do grupo. */
	private int consumidores() throws Exception {
		return admin.describeConsumerGroups(List.of(grupo)).all().get().get(grupo).members().size();
	}
}
