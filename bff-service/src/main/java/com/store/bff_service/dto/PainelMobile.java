package com.store.bff_service.dto;

import java.util.List;

/**
 * O DTO <b>enxuto</b>: so o que cabe na tela do celular.
 *
 * <p>Tres campos contra onze do {@link PainelWeb}. Nao ha offsets, nem lista de
 * processadas, nem UUID — a tela mobile mostra o lag, o estado do consumidor e
 * as ultimas vendas, e nada mais. Menos bytes na rede e menos bateria.
 *
 * @author guilherme.sales
 */
public record PainelMobile(long lag, boolean consumidorAtivo, List<VendaResumo> ultimasVendas) {
}
