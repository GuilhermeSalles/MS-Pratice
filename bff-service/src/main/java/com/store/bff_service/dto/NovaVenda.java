package com.store.bff_service.dto;

/**
 * O que o formulario manda. O id nao vem da tela: quem gera e o BFF.
 *
 * @author guilherme.sales
 */
public record NovaVenda(String nome, String produto) {
}
