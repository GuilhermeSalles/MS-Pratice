package com.store.bff_service.service;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.store.bff_service.client.VendaClient;
import com.store.bff_service.dto.NovaVenda;
import com.store.bff_service.dto.Venda;

/**
 * Gera o id da venda e publica pelo venda-service.
 *
 * <p>Os dois BFFs (web e mobile) usam esta mesma classe. E a mitigacao classica
 * da duplicacao entre BFFs: a orquestracao comum fica num lugar so, e cada
 * controller cuida apenas do <b>formato</b> que a sua tela precisa.
 *
 * @author guilherme.sales
 */
@Service
public class RegistroDeVendas {

	private static final int LIMITE = 50;
	private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

	private final VendaClient vendaClient;

	/** Historico da sessao. So memoria: o projeto nao tem banco de dados. */
	private final List<Venda> historico = new ArrayList<>();

	public RegistroDeVendas(VendaClient vendaClient) {
		this.vendaClient = vendaClient;
	}

	public Venda registrar(NovaVenda nova) {
		// O id nunca vem da tela: cada venda ganha um UUID novo aqui
		Venda venda = new Venda(UUID.randomUUID().toString(), texto(nova.nome(), "Cliente"),
				texto(nova.produto(), "Produto"), LocalTime.now().format(HORA));

		vendaClient.publicar(venda);
		guardar(venda);
		return venda;
	}

	/** Da mais recente para a mais antiga. */
	public synchronized List<Venda> historico() {
		return List.copyOf(historico);
	}

	private synchronized void guardar(Venda venda) {
		historico.add(0, venda);
		if (historico.size() > LIMITE) {
			historico.remove(LIMITE);
		}
	}

	private String texto(String valor, String padrao) {
		return valor == null || valor.isBlank() ? padrao : valor.trim();
	}
}
