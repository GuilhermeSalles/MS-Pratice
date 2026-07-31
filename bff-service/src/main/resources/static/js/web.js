/*
 * Tela web — consome GET /bff/web/painel, o DTO rico.
 *
 * A tela nao orquestra nada: uma chamada por segundo, e desenha o que veio.
 */

const CELULAS_VISIVEIS = 40;
const INTERVALO = 1200;

const el = (id) => document.getElementById(id);

let lidosAntes = null;   // para detectar o lote processado de uma vez
let ultimoLog = '';      // para so redesenhar o log quando os numeros mudam

/* ------------------------------------------------------- desenhar a tela --- */

function desenharLog(kafka) {
	const chave = `${kafka.gravados}-${kafka.lidos}`;
	if (chave === ultimoLog) return;
	ultimoLog = chave;

	const inicio = Math.max(0, kafka.gravados - CELULAS_VISIVEIS);
	el('celulas').innerHTML = '';

	for (let offset = inicio; offset < kafka.gravados; offset++) {
		const celula = document.createElement('div');
		celula.className = offset < kafka.lidos ? 'celula lida' : 'celula pendente';
		celula.textContent = offset;
		el('celulas').appendChild(celula);
	}

	el('vazio').hidden = kafka.gravados > 0;
	el('trilho').scrollLeft = el('trilho').scrollWidth;
}

function desenharNumeros(kafka) {
	el('n-gravados').textContent = kafka.gravados;
	el('n-lidos').textContent = kafka.lidos;
	el('n-lag').textContent = kafka.lag;
	el('n-consumidores').textContent = kafka.consumidores;
	el('bloco-lag').dataset.alerta = kafka.lag > 0;
	marcarSelo(el('selo-broker'), kafka.brokerOnline ? 'ativo' : 'offline',
		kafka.brokerOnline ? 'broker no ar' : 'broker fora do ar');
}

function desenharConsumidor(painel) {
	const estado = !painel.estoqueOnline ? 'offline' : (painel.consumidorAtivo ? 'ativo' : 'parado');

	marcarSelo(el('selo-consumidor'), estado, {
		offline: 'consumidor fora do ar',
		parado: 'consumidor parado',
		ativo: 'consumidor lendo'
	}[estado]);

	el('chave').setAttribute('aria-checked', painel.consumidorAtivo);
	el('chave').disabled = !painel.estoqueOnline;
	el('chave-rotulo').textContent = !painel.estoqueOnline
		? 'estoque-service fora do ar'
		: (painel.consumidorAtivo ? 'Derrubar consumidor' : 'Subir consumidor');

	el('nota').textContent = {
		offline: 'O processo do estoque não responde. Registre vendas mesmo assim: o Kafka guarda tudo e ele lê quando voltar.',
		parado: 'O listener saiu do grupo. As vendas continuam sendo publicadas e o lag cresce — nada se perde.',
		ativo: 'Derrube o consumidor, registre algumas vendas e veja o lag crescer. Ao subir, ele lê tudo de uma vez — do offset onde parou, sem repetir o que já tinha confirmado.'
	}[estado];
}

function marcarSelo(selo, estado, texto) {
	selo.dataset.estado = estado;
	selo.querySelector('span').textContent = texto;
}

function desenharLista(lista, vazio, contador, vendas) {
	el(contador).textContent = vendas.length;
	el(vazio).hidden = vendas.length > 0;
	el(lista).replaceChildren(...vendas.map(item));
}

function item(venda) {
	const li = document.createElement('li');
	li.className = 'evento';
	li.append(campo('nome', venda.nome), campo('hora', venda.horario), campo('produto', venda.produto));
	return li;
}

function campo(classe, texto) {
	const span = document.createElement('span');
	span.className = classe;
	span.textContent = texto;
	return span;
}

function avisar(quantidade) {
	el('aviso').innerHTML = `<b>${quantidade}</b> eventos processados de uma vez`;
	el('aviso').hidden = false;
	setTimeout(() => { el('aviso').hidden = true; }, 4000);
}

/* ---------------------------------------------------------------- fluxo --- */

async function atualizar() {
	let painel;
	try {
		painel = await (await fetch('/bff/web/painel')).json();
	} catch (e) {
		marcarSelo(el('selo-broker'), 'offline', 'bff fora do ar');
		return;
	}

	desenharLog(painel.kafka);
	desenharNumeros(painel.kafka);
	desenharConsumidor(painel);
	desenharLista('publicadas', 'vazio-publicadas', 'conta-publicadas', painel.publicadas);
	desenharLista('processadas', 'vazio-processadas', 'conta-processadas', painel.processadas);

	// O momento que o projeto existe para mostrar
	const lote = painel.kafka.lidos - lidosAntes;
	if (lidosAntes !== null && lote > 1) {
		avisar(lote);
	}
	lidosAntes = painel.kafka.lidos;
}

/* ---------------------------------------------------------------- acoes --- */

el('form').addEventListener('submit', async (evento) => {
	evento.preventDefault();
	el('erro').hidden = true;
	el('enviar').disabled = true;

	try {
		const resposta = await fetch('/bff/web/vendas', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ nome: el('nome').value, produto: el('produto').value })
		});
		if (!resposta.ok) throw new Error('O venda-service não respondeu. Confira se ele está no ar na porta 8081.');
		await atualizar();
	} catch (e) {
		el('erro').textContent = e.message;
		el('erro').hidden = false;
	} finally {
		el('enviar').disabled = false;
		el('produto').focus();
	}
});

el('chave').addEventListener('click', async () => {
	const ativo = el('chave').getAttribute('aria-checked') === 'true';
	await fetch(ativo ? '/bff/web/consumidor/parar' : '/bff/web/consumidor/iniciar', { method: 'POST' });
	await atualizar();
});

atualizar();
setInterval(atualizar, INTERVALO);
