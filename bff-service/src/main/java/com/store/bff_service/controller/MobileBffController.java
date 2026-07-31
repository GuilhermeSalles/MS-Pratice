package com.store.bff_service.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.store.bff_service.client.EstoqueClient;
import com.store.bff_service.dto.EstadoConsumidor;
import com.store.bff_service.dto.NovaVenda;
import com.store.bff_service.dto.PainelMobile;
import com.store.bff_service.dto.VendaResumo;
import com.store.bff_service.kafka.KafkaMonitor;
import com.store.bff_service.service.RegistroDeVendas;

/**
 * BFF da <b>tela mobile</b>: mesmos microsservicos, resposta enxuta.
 *
 * <p>Repare em duas diferencas para o {@link WebBffController}, e as duas vem
 * da tela, nao da regra de negocio:
 *
 * <ul>
 * <li>devolve {@link VendaResumo} em vez de {@code Venda} — sem UUID, sem nome
 * do cliente, sem offsets;
 * <li>nem chega a chamar {@code estoque.processadas()}, porque a tela pequena
 * nao mostra essa lista — <b>uma chamada a menos</b> para o downstream.
 * </ul>
 *
 * @author guilherme.sales
 */
@RestController
@RequestMapping("/bff/mobile")
public class MobileBffController {

	private static final int ULTIMAS = 5;

	private final RegistroDeVendas vendas;
	private final EstoqueClient estoque;
	private final KafkaMonitor kafka;

	public MobileBffController(RegistroDeVendas vendas, EstoqueClient estoque, KafkaMonitor kafka) {
		this.vendas = vendas;
		this.estoque = estoque;
		this.kafka = kafka;
	}

	/** Devolve so o que a tela mostra depois de salvar. */
	@PostMapping("/vendas")
	public VendaResumo registrar(@RequestBody NovaVenda nova) {
		return VendaResumo.de(vendas.registrar(nova));
	}

	@GetMapping("/painel")
	public PainelMobile painel() {
		List<VendaResumo> ultimas = vendas.historico().stream()
				.limit(ULTIMAS)
				.map(VendaResumo::de)
				.toList();

		return new PainelMobile(kafka.ler().lag(), estoque.estado().ativo(), ultimas);
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
