/*
 * Tela mobile — consome GET /bff/mobile/painel, o DTO enxuto.
 *
 * Compare com o web.js: aqui a resposta tem 3 campos (lag, consumidorAtivo,
 * ultimasVendas), entao o codigo da tela e menor tambem. E o mesmo Kafka por
 * tras — o que muda e o BFF que serve cada tela.
 */

const INTERVALO = 1500;

const el = (id) => document.getElementById(id);

let lagAntes = null;

/* ------------------------------------------------------- desenhar a tela --- */

function desenhar(painel) {
	el('n-lag').textContent = painel.lag;
	el('cartao-lag').dataset.alerta = painel.lag > 0;

	el('explica').textContent = painel.lag > 0
		? 'Publicadas e guardadas no Kafka. O estoque processa quando voltar.'
		: 'Tudo processado. O consumidor está em dia com o tópico.';

	el('selo').dataset.estado = painel.consumidorAtivo ? 'ativo' : 'parado';
	el('selo').querySelector('span').textContent = painel.consumidorAtivo ? 'consumidor lendo' : 'consumidor parado';

	el('chave').setAttribute('aria-checked', painel.consumidorAtivo);
	el('chave-rotulo').textContent = painel.consumidorAtivo ? 'Derrubar consumidor' : 'Subir consumidor';

	el('vazio').hidden = painel.ultimasVendas.length > 0;
	el('vendas').replaceChildren(...painel.ultimasVendas.map(item));
}

function item(venda) {
	const li = document.createElement('li');
	li.className = 'venda';
	li.append(campo('produto', venda.produto), campo('hora', venda.horario));
	return li;
}

function campo(classe, texto) {
	const span = document.createElement('span');
	span.className = classe;
	span.textContent = texto;
	return span;
}

/* ---------------------------------------------------------------- fluxo --- */

async function atualizar() {
	let painel;
	try {
		painel = await (await fetch('/bff/mobile/painel')).json();
	} catch (e) {
		el('selo').dataset.estado = 'offline';
		el('selo').querySelector('span').textContent = 'sem conexão';
		return;
	}

	desenhar(painel);

	// O lag caindo de uma vez e o consumidor voltando e lendo tudo
	const lote = lagAntes - painel.lag;
	if (lagAntes !== null && lote > 1) {
		el('aviso').innerHTML = `<b>${lote}</b> vendas processadas de uma vez`;
		el('aviso').hidden = false;
		setTimeout(() => { el('aviso').hidden = true; }, 4000);
	}
	lagAntes = painel.lag;
}

/* ---------------------------------------------------------------- acoes --- */

el('form').addEventListener('submit', async (evento) => {
	evento.preventDefault();
	el('erro').hidden = true;
	el('enviar').disabled = true;

	try {
		const resposta = await fetch('/bff/mobile/vendas', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ nome: el('nome').value, produto: el('produto').value })
		});
		if (!resposta.ok) throw new Error('Não foi possível registrar. Confira se o venda-service está no ar.');
		await atualizar();
	} catch (e) {
		el('erro').textContent = e.message;
		el('erro').hidden = false;
	} finally {
		el('enviar').disabled = false;
	}
});

el('chave').addEventListener('click', async () => {
	const ativo = el('chave').getAttribute('aria-checked') === 'true';
	await fetch(ativo ? '/bff/mobile/consumidor/parar' : '/bff/mobile/consumidor/iniciar', { method: 'POST' });
	await atualizar();
});

atualizar();
setInterval(atualizar, INTERVALO);
