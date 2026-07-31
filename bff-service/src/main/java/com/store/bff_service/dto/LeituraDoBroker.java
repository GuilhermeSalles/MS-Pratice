package com.store.bff_service.dto;

/**
 * O retrato do topico lido direto do Kafka.
 *
 * @param gravados      eventos ja escritos no log (log-end-offset)
 * @param lidos         ate onde o grupo confirmou leitura (offset commitado)
 * @param lag           gravados - lidos: o que existe publicado e ainda nao lido
 * @param consumidores  consumidores conectados ao grupo agora
 * @param brokerOnline  se o Kafka respondeu
 *
 * @author guilherme.sales
 */
public record LeituraDoBroker(String topico, String grupo, long gravados, long lidos, long lag, int consumidores,
		boolean brokerOnline) {
}
