package com.store.bff_service.dto;

import java.util.List;

/**
 * O DTO <b>rico</b>: tudo que a tela grande do desktop aproveita.
 *
 * <p>Offsets, estado do consumidor e as duas listas lado a lado — publicadas
 * contra processadas.
 *
 * @author guilherme.sales
 */
public record PainelWeb(LeituraDoBroker kafka, boolean estoqueOnline, boolean consumidorAtivo, List<Venda> publicadas,
		List<Venda> processadas) {
}
