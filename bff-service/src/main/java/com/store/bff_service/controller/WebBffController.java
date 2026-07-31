package com.store.bff_service.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.store.bff_service.client.EstoqueClient;
import com.store.bff_service.dto.EstadoConsumidor;
import com.store.bff_service.dto.NovaVenda;
import com.store.bff_service.dto.PainelWeb;
import com.store.bff_service.dto.Venda;
import com.store.bff_service.kafka.KafkaMonitor;
import com.store.bff_service.service.RegistroDeVendas;

/**
 * BFF da <b>tela web</b>: agrega tres fontes e devolve o DTO rico.
 *
 * @author guilherme.sales
 */
@RestController
@RequestMapping("/bff/web")
public class WebBffController {

	private final RegistroDeVendas vendas;
	private final EstoqueClient estoque;
	private final KafkaMonitor kafka;

	public WebBffController(RegistroDeVendas vendas, EstoqueClient estoque, KafkaMonitor kafka) {
		this.vendas = vendas;
		this.estoque = estoque;
		this.kafka = kafka;
	}

	@PostMapping("/vendas")
	public Venda registrar(@RequestBody NovaVenda nova) {
		return vendas.registrar(nova);
	}

	@GetMapping("/painel")
	public PainelWeb painel() {
		EstadoConsumidor consumidor = estoque.estado();

		// Agrega: broker + estoque + historico, num payload so
		return new PainelWeb(kafka.ler(), consumidor.online(), consumidor.ativo(), vendas.historico(),
				estoque.processadas());
	}

	@PostMapping("/consumidor/parar")
	public EstadoConsumidor parar() {
		return estoque.parar();
	}

	@PostMapping("/consumidor/iniciar")
	public EstadoConsumidor iniciar() {
		return estoque.iniciar();
	}
}
