package com.store.estoque_service.model;

/**
 * Um evento de venda que o listener ja consumiu do topico.
 *
 * <p>Fica apenas em memoria: o projeto nao usa banco de dados. Se o servico
 * reiniciar a lista some, mas o offset do grupo continua no broker — e e isso
 * que garante que nada seja reprocessado nem perdido.
 *
 * @author guilherme.sales
 */
public record EventoProcessado(String id, String nome, String produto, String horario, String bruto) {
}
