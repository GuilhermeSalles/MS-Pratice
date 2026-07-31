package com.store.bff_service.dto;

/**
 * Os dois jeitos de o consumidor estar fora: o processo caiu ({@code online}
 * falso), ou o processo esta no ar mas o listener foi parado.
 *
 * @author guilherme.sales
 */
public record EstadoConsumidor(boolean online, boolean ativo) {

	public static EstadoConsumidor offline() {
		return new EstadoConsumidor(false, false);
	}
}
