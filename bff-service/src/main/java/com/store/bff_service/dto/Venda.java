package com.store.bff_service.dto;

/**
 * Uma venda completa — o formato que a <b>tela web</b> recebe.
 *
 * @author guilherme.sales
 */
public record Venda(String id, String nome, String produto, String horario) {
}
