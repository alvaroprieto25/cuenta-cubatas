/* ==============================================================
   Cuenta Cubatas v2 — conexión robusta con failover
   ============================================================== */

/* ---------- Catch global: muestra errores en vez de quedar en blanco ---------- */
window.addEventListener('error', (ev) => {
  const t = document.getElementById('toast');
  if (t){
    t.textContent = '⚠️ Error: ' + (ev.message || 'desconocido');
    t.classList.add('show');
  }
});
window.addEventListener('unhandledrejection', (ev) => {
  console.warn('Unhandled rejection:', ev.reason);
});

/* ---------- CONFIG ---------- */
const BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt'
];
const BASE           = 'cuenta-cubatas-v2';
const EVENTS_TOPIC   = BASE + '/events';
const ITEMS_TOPIC    = eid => `${BASE}/events/${eid}/items`;
const PLAYER_TOPIC   = (eid,pid) => `${BASE}/events/${eid}/players/${pid}`;
const PLAYERS_PREFIX = eid => `${BASE}/events/${eid}/players/`;

/* Cambia esta contraseña — solo protege la UI */
const ADMIN_PASSWORD = 'cubatas2025';

const AVS = ['🦁','🐯','🦊','🐺','🐸','🦄','🐙','🦖','🐧','🤠','😎','🥳','🐉','🐨','🐮','🦉'];

const DEFAULT_ITEMS = {
  cana:     { emoji:'🍺', name:'Caña',     points:1 },
  rebujito: { emoji:'🌸', name:'Rebujito', points:1 },
  vino:     { emoji:'🍷', name:'Vino',     points:1 },
  copa:     { emoji:'🍸', name:'Copazo',   points:2 },
  chupito:  { emoji:'🥃', name:'Chupito',  points:1 },
  agua:     { emoji:'💧', name:'Agua',     points:0 }
};

/* ---------- ESTADO ---------- */
let client       = null;
let connected    = false;
let ready        = false;
let events       = {};
let items        = {};
let players      = {};
let currentEventId = null;
let subscribedEid  = null;
let currentView    = 'list';

let meId    = null;
let avatar  = AVS[0];
let isAdmin = false;
let lastK   = null;
let feed    = [];
let lastLeader = null;

const $   = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

/* ==============================================================
   TOAST / CONFETI / OVERLAY
   ============================================================== */
let toastTimer;
function toast(msg){
  const t = $('toast');
  if (!t) return;
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 2200);
}

function confetti(set){
  for (let i=0;i<26;i++){
    const e = document.createElement('div');
    e.className = 'fx'; e.textContent = set[i % set.length];
    e.style.left = Math.random()*100 + 'vw';
    e.style.animationDelay = Math.random()*.8 + 's';
    document.body.appendChild(e);
    setTimeout(() => e.remove(), 3400);
  }
}

function showOverlay(text, opts = {}){
  $('overlay').classList.remove('hide');
  $('app').classList.add('hide');
  $('overlay-text').textContent = text;
  $('overlay-spinner').classList.toggle('hide', !(opts.spinner !== false));
  $('overlay-retry').classList.toggle('hide', !opts.retry);
}

function hideOverlay(){
  $('overlay').classList.add('hide');
  $('app').classList.remove('hide');
}

function updateOverlay(){
  if (ready){ hideOverlay(); return; }
  if (connected) showOverlay('Sincronizando…');
  else showOverlay('Conectando…');
}

/* ==============================================================
   PUNTOS
   ============================================================== */
function scoreOf(p){
  if (!p || !p.c) return 0;
  return Object.keys(p.c).reduce((s,k) => {
    const it = items[k];
    return s + (it ? (it.points || 0) * p.c[k] : 0);
  }, 0);
}
function totalDrinks(p){
  return Object.values((p && p.c) || {}).reduce((s,n)=>s+n,0);
}
function streak(p){
  return ((p && p.h) || []).filter(t => Date.now() - t < 20*60000).length;
}
function sortedPlayers(){
  return Object.keys(players)
    .map(id => Object.assign({ id }, players[id], { score: scoreOf(players[id]) }))
    .sort((a,b) => b.score - a.score || ((a.last && a.last.t)||0) - ((b.last && b.last.t)||0));
}

/* ==============================================================
   MQTT — CONEXIÓN RÁPIDA + FAILOVER
   ============================================================== */
async function connectMqtt(){
  if (client && client.connected) return;

  if (client){ try { client.end(true); } catch(e){} client = null; }
  connected = false;
  ready     = false;
  updateOverlay();

  for (let i=0;i<BROKERS.length;i++){
    const url = BROKERS[i];
    try {
      await tryBroker(url);
      return;
    } catch(e){
      console.warn('Broker falló:', url, e && e.message || e);
    }
  }

  showOverlay('No se puede conectar. Revisa tu red.', { spinner:false, retry:true });
  $('overlay-retry').onclick = () => {
    $('overlay-retry').onclick = null;
    connectMqtt();
  };
}

function tryBroker(url){
  return new Promise((resolve, reject) => {
    if (typeof mqtt === 'undefined'){
      return reject(new Error('MQTT no cargado'));
    }

    const c = mqtt.connect(url, {
      clientId:        'cc_' + Math.random().toString(16).slice(2,10),
      clean:           true,
      connectTimeout:  5000,
      reconnectPeriod: 1500,
      keepalive:       30,
      resubscribe:     true
    });

    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { c.end(true); } catch(e){}
      reject(new Error('timeout'));
    }, 8000);

    c.on('message', handleMessage);

    c.on('connect', () => {
      const subs = [{ topic: EVENTS_TOPIC, qos: 0 }];
      if (subscribedEid){
        subs.push({ topic: ITEMS_TOPIC(subscribedEid), qos: 0 });
        subs.push({ topic: PLAYERS_PREFIX(subscribedEid) + '+', qos: 0 });
      }

      c.subscribe(subs, (err) => {
        connected = true;

        if (!settled){
          if (err){
            settled = true;
            clearTimeout(timer);
            try { c.end(true); } catch(e){}
            return reject(err);
          }
          settled = true;
          clearTimeout(timer);
          client  = c;
          ready   = true;
          hideOverlay();
          render();
          resolve();
        } else {
          hideOverlay();
        }
      });
    });

    c.on('reconnect', () => { connected = false; updateOverlay(); });
    c.on('offline',   () => { connected = false; updateOverlay(); });
    c.on('close',     () => { connected = false; updateOverlay(); });
    c.on('error', (e) => {
      console.warn('MQTT error:', e);
      if (!settled){
        settled = true;
        clearTimeout(timer);
        try { c.end(true); } catch(err){}
        reject(e);
      }
    });
  });
}

/* ==============================================================
   MANEJO DE MENSAJES
   ============================================================== */
function handleMessage(topic, payload){
  let data;
  try { data = JSON.parse(payload.toString()); } catch(e){ return; }

  if (topic === EVENTS_TOPIC){
    events = data || {};
    if (currentEventId && !events[currentEventId]){
      toast('Este evento ya no existe');
      backToList();
      return;
    }
    if (ready) render();
    return;
  }

  if (!currentEventId) return;

  if (topic === ITEMS_TOPIC(currentEventId)){
    items = data || {};
    if (ready) renderEvent();
    return;
  }

  const prefix = PLAYERS_PREFIX(currentEventId);
  if (topic.indexOf(prefix) === 0){
    const pid = topic.slice(prefix.length);
    if (!pid || pid.indexOf('/') !== -1) return;
    if (!data || !data.name) return;

    if (pid === meId){
      const mine = players[meId];
      if (mine && mine.last && data.last && mine.last.t && data.last.t &&
          data.last.t < mine.last.t){
        publishPlayer(currentEventId, meId, mine);
        return;
      }
    }

    if (ready && pid !== meId && data.last && data.last.k){
      const prev = players[pid];
      const oldT = (prev && prev.last && prev.last.t) || 0;
      if (data.last.t > oldT){
        const d = items[data.last.k];
        if (d){
          const txt = d.points === 0
            ? data.av + ' <b>' + esc(data.name) + '</b> se ha pedido ' + d.emoji + ' ' + esc(d.name) + ' (el sensato)'
            : data.av + ' <b>' + esc(data.name) + '</b> se ha tomado ' + d.emoji + ' ' + esc(d.name) + ' (+' + d.points + ')';
          feed.unshift(txt);
          if (feed.length > 6) feed.pop();
          updateFeed();
        }
      }
    }

    players[pid] = data;
    if (ready) renderEvent();
  }
}

/* ==============================================================
   PUBLICAR
   ============================================================== */
function publishEvents(){
  if (!client || !client.connected) return toast('Sin conexión');
  client.publish(EVENTS_TOPIC, JSON.stringify(events), { qos:0, retain:true });
}
function publishItems(){
  if (!client || !client.connected) return toast('Sin conexión');
  client.publish(ITEMS_TOPIC(currentEventId), JSON.stringify(items), { qos:0, retain:true });
}
function publishPlayer(eid, pid, data){
  if (!client || !client.connected) return toast('Sin conexión');
  client.publish(PLAYER_TOPIC(eid, pid), JSON.stringify(data), { qos:0, retain:true });
  try { localStorage.setItem('cc_player_' + eid, JSON.stringify(data)); } catch(e){}
}

/* ==============================================================
   RENDER — LISTA
   ============================================================== */
function render(){
  if (!ready) return;
  if (currentView === 'list') renderList();
  else renderEvent();
}

function renderList(){
  $('view-list').classList.remove('hide');
  $('view-event').classList.add('hide');

  $('btn-admin-login').classList.toggle('hide', isAdmin);
  $('btn-admin-logout').classList.toggle('hide', !isAdmin);
  $('btn-new-event').classList.toggle('hide', !isAdmin);
  $('admin-state').textContent = isAdmin ? '✅ Modo admin' : 'Modo jugador';

  const evs = Object.keys(events)
    .map(id => Object.assign({ id }, events[id]))
    .sort((a,b) => {
      const aOpen = a.status !== 'finished';
      const bOpen = b.status !== 'finished';
      if (aOpen !== bOpen) return aOpen ? -1 : 1;
      return (b.createdAt || 0) - (a.createdAt || 0);
    });

  $('events-list').innerHTML = evs.length
    ? evs.map(e => {
        const dateStr = e.createdAt
          ? new Date(e.createdAt).toLocaleDateString('es-ES',{day:'2-digit',month:'short',year:'numeric'})
          : '';
        const isFin = e.status === 'finished';
        return '<button class="event" data-open="' + e.id + '">'
          + '<div>'
          +   '<div class="event-name">' + esc(e.name || 'Evento') + '</div>'
          +   '<div class="sub" style="margin:2px 0 0;text-align:left">' + dateStr + '</div>'
          + '</div>'
          + '<span class="badge ' + (isFin ? 'bad' : 'ok') + '">'
          +   (isFin ? 'Finalizado' : 'Abierto')
          + '</span>'
          + '</button>';
      }).join('')
    : '<div class="card" style="text-align:center;color:var(--mut)">'
      + (isAdmin ? 'Pulsa “+ Nuevo evento” para empezar.' : 'Todavía no hay eventos.')
      + '</div>';

  const btns = document.querySelectorAll('[data-open]');
  for (let i=0;i<btns.length;i++){
    btns[i].addEventListener('click', (function(id){
      return function(){ openEvent(id); };
    })(btns[i].dataset.open));
  }
}

/* ==============================================================
   RENDER — EVENTO
   ============================================================== */
function renderEvent(){
  $('view-list').classList.add('hide');
  $('view-event').classList.remove('hide');

  const ev = events[currentEventId];
  if (!ev){ backToList(); return; }

  const isOpen = ev.status !== 'finished';
  const mine   = players[meId];

  $('ev-name').textContent = ev.name || 'Evento';
  $('ev-status').innerHTML = '<span class="badge ' + (isOpen ? 'ok' : 'bad') + '">'
    + (isOpen ? 'Abierto' : 'Finalizado') + '</span>';

  $('ev-join').classList.toggle('hide', !!mine || !isOpen);
  $('ev-content').classList.toggle('hide', !mine);

  if (!mine){ renderAvatarPicker(); return; }

  $('meAv').textContent = mine.av || '🍹';
  $('meNm').textContent = mine.name;
  $('mePts').textContent = scoreOf(mine);
  const list = sortedPlayers();
  const rank = list.findIndex(p => p.id === meId) + 1;
  $('meSt').textContent = 'Nº ' + rank + ' de ' + list.length + ' · total ' + totalDrinks(mine) + ' bebidas';

  const keys = Object.keys(items);
  if (keys.length){
    $('btns').innerHTML = keys.map(id => {
      const it = items[id];
      return '<button class="dk" data-k="' + id + '"' + (!isOpen ? ' disabled' : '') + '>'
        + '<b>' + (it.emoji || '🍹') + '</b>' + esc(it.name) + '<br>'
        + '<small>' + (it.points ? '+'+it.points : 'bonus') + '</small>'
        + '</button>';
    }).join('');
  } else {
    $('btns').innerHTML = '<div class="sub" style="grid-column:1/-1">'
      + (isAdmin ? 'Añade ítems en el panel ↓' : 'El admin aún no ha añadido bebidas.')
      + '</div>';
  }

  $('warn').innerHTML = (streak(mine) >= 3 && isOpen)
    ? '<div class="warn">🔥 Racha: 3+ en 20 min. ¡A por la corona! 👑</div>' : '';

  renderRanking(list);
  renderAdminPanel(isOpen);

  const dkButtons = document.querySelectorAll('#btns .dk');
  for (let i=0;i<dkButtons.length;i++){
    dkButtons[i].addEventListener('click', (function(k){
      return function(){ drink(k); };
    })(dkButtons[i].dataset.k));
  }
}

function renderAvatarPicker(){
  const box = $('avs');
  if (box.dataset.ready) return;
  box.dataset.ready = '1';
  box.innerHTML = AVS.map(a =>
    '<button class="av' + (a===avatar?' on':'') + '" data-av="' + a + '">' + a + '</button>'
  ).join('');
  box.onclick = (e) => {
    const b = e.target.closest('[data-av]');
    if (!b) return;
    avatar = b.dataset.av;
    try { localStorage.setItem('cc_avatar', avatar); } catch(e){}
    const all = box.querySelectorAll('.av');
    for (let i=0;i<all.length;i++) all[i].classList.toggle('on', all[i] === b);
  };
}

function renderRanking(list){
  const max = Math.max.apply(null, [1].concat(list.map(p => p.score)));

  const top = [list[1], list[0], list[2]];
  const hs  = [95, 125, 75];
  $('pod').innerHTML = top.map((p,i) => {
    if (!p) return '<div class="pc"></div>';
    return '<div class="pc">'
      + '<div class="em">' + (p.av || '🍹') + '</div>'
      + '<div class="nm">' + esc(p.name) + '</div>'
      + '<div class="bar" style="height:' + (hs[i]*(0.35+0.65*p.score/max)) + 'px">'
      + ['🥈','🥇','🥉'][i] + ' ' + p.score
      + '</div></div>';
  }).join('');

  const water = list.slice().sort((a,b) =>
    ((b.c && b.c.agua)||0) - ((a.c && a.c.agua)||0))[0];

  function badges(p){
    let b = '';
    if (p.id === list[0].id && p.score > 0) b += '<span class="bd">👑 líder</span>';
    if (water && p.id === water.id && water.c && (water.c.agua||0) > 0) b += '<span class="bd">💧 sensato</span>';
    if (streak(p) >= 3) b += '<span class="bd">🔥 racha</span>';
    if (list.length > 2 && p.id === list[list.length-1].id) b += '<span class="bd">🐢 tortuga</span>';
    return b;
  }

  $('rows').innerHTML = list.map((p,i) =>
    '<div class="row">'
    + '<div class="rk">' + (i+1) + '</div>'
    + '<div class="em">' + (p.av || '🍹') + '</div>'
    + '<div class="in">'
    +   '<div class="nm">' + esc(p.name) + (p.id===meId?' (tú)':'') + ' ' + badges(p) + '</div>'
    +   '<div class="tr"><i style="width:' + (p.score/max*100) + '%"></i></div>'
    + '</div>'
