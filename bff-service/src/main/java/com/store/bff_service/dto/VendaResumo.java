package com.store.bff_service.dto;

/**
 * A mesma venda, enxuta — o formato que a <b>tela mobile</b> recebe.
 *
 * <p>Sem o UUID e sem o nome do cliente: a tela pequena nao mostra nenhum dos
 * dois, entao nao faz sentido gastar rede com eles.
 *
 * @author guilherme.sales
 */
public record VendaResumo(String produto, String horario) {

	public static VendaResumo de(Venda venda) {
		return new VendaResumo(venda.produto(), venda.horario());
	}
}
