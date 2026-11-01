/* ==============================================================
   Cuenta Cubatas v2 — conexión robusta y rápida
   ============================================================== */

/* ---------- CONFIG ---------- */
const BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt'
];
const BASE             = 'cuenta-cubatas-v2';
const EVENTS_TOPIC     = BASE + '/events';
const ITEMS_TOPIC      = eid => `${BASE}/events/${eid}/items`;
const PLAYER_TOPIC     = (eid,pid) => `${BASE}/events/${eid}/players/${pid}`;
const PLAYERS_PREFIX   = eid => `${BASE}/events/${eid}/players/`;

/* Cambia esta contraseña — solo protege la UI (no es seguridad real) */
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
let client         = null;
let connected      = false;   // broker conectado ahora
let ready          = false;   // UI desbloqueada
let firstAttempt   = true;

let events         = {};
let items          = {};
let players        = {};
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
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 2200);
}

function confetti(set){
  for(let i=0;i<26;i++){
    const e = document.createElement('div');
    e.className = 'fx'; e.textContent = set[i % set.length];
    e.style.left = Math.random()*100 + 'vw';
    e.style.animationDelay = Math.random()*.8 + 's';
    document.body.appendChild(e);
    setTimeout(() => e.remove(), 3400);
  }
}

function showOverlay(text, { spinner=true, retry=false } = {}){
  $('overlay').classList.remove('hide');
  $('app').classList.add('hide');
  $('overlay-text').textContent = text;
  $('overlay-spinner').classList.toggle('hide', !spinner);
  $('overlay-retry').classList.toggle('hide', !retry);
}

function hideOverlay(){
  $('overlay').classList.add('hide');
  $('app').classList.remove('hide');
}

function updateOverlay(){
  if(ready){ hideOverlay(); return; }
  if(connected) showOverlay('Sincronizando…');
  else showOverlay('Conectando…');
}

/* ==============================================================
   PUNTOS
   ============================================================== */
function scoreOf(p){
  if(!p?.c) return 0;
  return Object.entries(p.c).reduce((s,[k,n]) => {
    const it = items[k];
    return s + (it ? (it.points || 0) * n : 0);
  }, 0);
}
function totalDrinks(p){
  return Object.values(p?.c || {}).reduce((s,n)=>s+n,0);
}
function streak(p){
  return (p.h || []).filter(t => Date.now() - t < 20*60000).length;
}
function sortedPlayers(){
  return Object.entries(players)
    .map(([id,p]) => ({ id, ...p, score: scoreOf(p) }))
    .sort((a,b) => b.score - a.score || (a.last?.t||0) - (b.last?.t||0));
}

/* ==============================================================
   MQTT — CONEXIÓN RÁPIDA + FAILOVER
   ============================================================== */
async function connectMqtt(){
  if (client?.connected) return;

  if (client){ try { client.end(true); } catch(e){} client = null; }
  connected = false;
  ready     = false;
  updateOverlay();

  for (const url of BROKERS){
    try {
      await tryBroker(url);
      return; // conexión ok
    } catch(e){
      console.warn('Broker falló:', url, e.message || e);
    }
  }

  // Todos fallaron
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
      reconnectPeriod: 1500,      // rápido tras caída
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

    /* Escuchar mensajes SIEMPRE (incluye retained al reconectar) */
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
          // Primera conexión
          if (err){
            settled = true;
            clearTimeout(timer);
            try { c.end(true); } catch(e){}
            return reject(err);
          }
          settled  = true;
          firstAttempt = false;
          clearTimeout(timer);
          client = c;
          ready  = true;
          hideOverlay();
          render();
          resolve();
        } else {
          // Reconexión tras caída
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

  /* --- EVENTOS --- */
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

  /* --- ÍTEMS del evento actual --- */
  if (topic === ITEMS_TOPIC(currentEventId)){
    items = data || {};
    if (ready) renderEvent();
    return;
  }

  /* --- JUGADORES del evento actual --- */
  const prefix = PLAYERS_PREFIX(currentEventId);
  if (topic.startsWith(prefix)){
    const pid = topic.slice(prefix.length);
    if (!pid || pid.includes('/')) return;
    if (!data || !data.name) return;

    /* Anti-duplicado: retained viejo nunca sobrescribe el estado local más nuevo */
    if (pid === meId){
      const mine = players[meId];
      if (mine?.last?.t && data.last?.t && data.last.t < mine.last.t){
        // Nuestro local es más nuevo → re-publicar para alinear broker
        publishPlayer(currentEventId, meId, mine);
        return;
      }
    }

    /* Feed “última hora” */
    if (ready && pid !== meId && data.last?.k){
      const prev = players[pid];
      const oldT = prev?.last?.t || 0;
      if (data.last.t > oldT){
        const d = items[data.last.k];
        if (d){
          const txt = d.points === 0
            ? `${data.av} <b>${esc(data.name)}</b> se ha pedido ${d.emoji} ${esc(d.name)} (el sensato)`
            : `${data.av} <b>${esc(data.name)}</b> se ha tomado ${d.emoji} ${esc(d.name)} (+${d.points})`;
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
  if (!client?.connected) return toast('Sin conexión');
  client.publish(EVENTS_TOPIC, JSON.stringify(events), { qos:0, retain:true });
}
function publishItems(){
  if (!client?.connected) return toast('Sin conexión');
  client.publish(ITEMS_TOPIC(currentEventId), JSON.stringify(items), { qos:0, retain:true });
}
function publishPlayer(eid, pid, data){
  if (!client?.connected) return toast('Sin conexión');
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

  const evs = Object.entries(events)
    .map(([id,e]) => ({ id, ...e }))
    .sort((a,b) => {
      const aOpen = a.status !== 'finished';
      const bOpen = b.status !== 'finished';
      if (aOpen !== bOpen) return aOpen ? -1 : 1;
      return (b.createdAt || 0) - (a.createdAt || 0);
    });

  $('events-list').innerHTML = evs.length
    ? evs.map(e => `
        <button class="event" data-open="${e.id}">
          <div>
            <div class="event-name">${esc(e.name || 'Evento')}</div>
            <div class="sub" style="margin:2px 0 0;text-align:left">
              ${e.createdAt ? new Date(e.createdAt).toLocaleDateString('es-ES',
                {day:'2-digit',month:'short',year:'numeric'}) : ''}
            </div>
          </div>
          <span class="badge ${e.status === 'finished' ? 'bad' : 'ok'}">
            ${e.status === 'finished' ? 'Finalizado' : 'Abierto'}
          </span>
        </button>`).join('')
    : `<div class="card" style="text-align:center;color:var(--mut)">
         ${isAdmin ? 'Pulsa “+ Nuevo evento” para empezar.' : 'Todavía no hay eventos.'}
       </div>`;

  document.querySelectorAll('[data-open]').forEach(b =>
    b.addEventListener('click', () => openEvent(b.dataset.open)));
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
  $('ev-status').innerHTML = `<span class="badge ${isOpen ? 'ok' : 'bad'}">
    ${isOpen ? 'Abierto' : 'Finalizado'}</span>`;

  $('ev-join').classList.toggle('hide', !!mine || !isOpen);
  $('ev-content').classList.toggle('hide', !mine);

  if (!mine){ renderAvatarPicker(); return; }

  $('meAv').textContent = mine.av || '🍹';
  $('meNm').textContent = mine.name;
  $('mePts').textContent = scoreOf(mine);
  const list = sortedPlayers();
  const rank = list.findIndex(p => p.id === meId) + 1;
  $('meSt').textContent = `Nº ${rank} de ${list.length} · total ${totalDrinks(mine)} bebidas`;

  $('btns').innerHTML = Object.entries(items).map(([id,it]) =>
    `<button class="dk" data-k="${id}" ${!isOpen ? 'disabled' : ''}>
       <b>${it.emoji || '🍹'}</b>${esc(it.name)}<br>
       <small>${it.points ? '+'+it.points : 'bonus'}</small>
     </button>`).join('')
    || `<div class="sub" style="grid-column:1/-1">
         ${isAdmin ? 'Añade ítems en el panel ↓' : 'El admin aún no ha añadido bebidas.'}
       </div>`;

  $('warn').innerHTML = (streak(mine) >= 3 && isOpen)
    ? '<div class="warn">🔥 Racha: 3+ en 20 min. ¡A por la corona! 👑</div>' : '';

  renderRanking(list);
  renderAdminPanel(isOpen);

  document.querySelectorAll('#btns .dk').forEach(b =>
    b.addEventListener('click', () => drink(b.dataset.k)));
}

function renderAvatarPicker(){
  const box = $('avs');
  if (box.dataset.ready) return;
  box.dataset.ready = '1';
  box.innerHTML = AVS.map(a =>
    `<button class="av${a===avatar?' on':''}" data-av="${a}">${a}</button>`).join('');
  box.onclick = (e) => {
    const b = e.target.closest('[data-av]');
    if (!b) return;
    avatar = b.dataset.av;
    try { localStorage.setItem('cc_avatar', avatar); } catch(e){}
    box.querySelectorAll('.av').forEach(x => x.classList.toggle('on', x === b));
  };
}

function renderRanking(list){
  const max = Math.max(1, ...list.map(p => p.score));

  const top = [list[1], list[0], list[2]], hs = [95,125,75];
  $('pod').innerHTML = top.map((p,i) => p
    ? `<div class="pc">
         <div class="em">${p.av || '🍹'}</div>
         <div class="nm">${esc(p.name)}</div>
         <div class="bar" style="height:${hs[i]*(0.35+0.65*p.score/max)}px">
           ${['🥈','🥇','🥉'][i]} ${p.score}
         </div>
       </div>`
    : '<div class="pc"></div>').join('');

  const water = [...list].sort((a,b)=>(b.c?.agua||0)-(a.c?.agua||0))[0];
  const badges = p => {
    let b = '';
    if (p.id === list[0].id && p.score > 0) b += '<span class="bd">👑 líder</span>';
    if (water && p.id === water.id && (water.c?.agua||0) > 0) b += '<span class="bd">💧 sensato</span>';
    if (streak(p) >= 3) b += '<span class="bd">🔥 racha</span>';
    if (list.length > 2 && p.id === list[list.length-1].id) b += '<span class="bd">🐢 tortuga</span>';
    return b;
  };

  $('rows').innerHTML = list.map((p,i) => `
    <div class="row">
      <div class="rk">${i+1}</div>
      <div class="em">${p.av || '🍹'}</div>
      <div class="in">
        <div class="nm">${esc(p.name)}${p.id===meId?' (tú)':''} ${badges(p)}</div>
        <div class="tr"><i style="width:${p.score/max*100}%"></i></div>
      </div>
      <div class="sc">${p.score}</div>
    </div>`).join('');

  if (list[0].score > 0 && lastLeader && lastLeader !== list[0].id){
    toast('👑 ¡' + list[0].name + ' toma el liderato!');
    confetti(['👑','🥂','🎉']);
  }
  lastLeader = list[0].score > 0 ? list[0].id : lastLeader;
}

function renderAdminPanel(isOpen){
  $('admin-panel').classList.toggle('hide', !isAdmin);
  if (!isAdmin) return;

  $('items-manage').innerHTML = Object.entries(items).map(([id,it]) => `
    <div class="row" style="padding:6px 0">
      <span class="em">${it.emoji || '🍹'}</span>
      <div class="in"><div class="nm">${esc(it.name)} <span class="bd">+${it.points}</span></div></div>
      <button class="sm" data-del-item="${id}" style="color:var(--rose)">Borrar</button>
    </div>`).join('')
    || `<div class="sub" style="margin:0;text-align:left">Sin ítems</div>`;

  document.querySelectorAll('[data-del-item]').forEach(b =>
    b.addEventListener('click', () => deleteItem(b.dataset.delItem)));

  $('btn-finish-event').classList.toggle('hide', !isOpen);
}

function updateFeed(){
  const f = $('feed');
  if (!f) return;
  f.innerHTML = feed.length
    ? feed.map(x => '<div>' + x + '</div>').join('')
    : '<div>Esperando el primer brindis…</div>';
}

/* ==============================================================
   NAVEGACIÓN
   ============================================================== */
async function openEvent(eid){
  currentEventId = eid;
  currentView    = 'event';
  items   = {};
  players = {};
  feed    = [];
  lastLeader = null;

  $('feed').innerHTML = '<div>Cargando…</div>';
  $('ev-join').classList.add('hide');
  $('ev-content').classList.add('hide');

  if (subscribedEid && subscribedEid !== eid){
    client.unsubscribe([ITEMS_TOPIC(subscribedEid), PLAYERS_PREFIX(subscribedEid) + '+']);
  }
  subscribedEid = eid;

  await new Promise(resolve => {
    client.subscribe([
      { topic: ITEMS_TOPIC(eid), qos: 0 },
      { topic: PLAYERS_PREFIX(eid) + '+', qos: 0 }
    ], () => resolve());
  });

  // Ventana corta para recibir retained
  await new Promise(r => setTimeout(r, 400));

  // Backup local (si el broker perdió retained)
  if (!players[meId]){
    try {
      const backup = JSON.parse(localStorage.getItem('cc_player_' + eid));
      if (backup && backup.name){
        players[meId] = backup;
        publishPlayer(eid, meId, backup);
      }
    } catch(e){}
  }

  renderEvent();
  updateFeed();
}

function backToList(){
  if (subscribedEid){
    client.unsubscribe([ITEMS_TOPIC(subscribedEid), PLAYERS_PREFIX(subscribedEid) + '+']);
    subscribedEid = null;
  }
  currentEventId = null;
  currentView    = 'list';
  items = {}; players = {}; feed = []; lastLeader = null;
  render();
}

/* ==============================================================
   ACCIONES JUGADOR
   ============================================================== */
function drink(k){
  if (!ready || !client?.connected) return toast('Sin conexión');
  const ev = events[currentEventId];
  if (!ev || ev.status === 'finished') return;
  const mine = players[meId];
  if (!mine) return toast('Únete primero');
  const it = items[k];
  if (!it) return;

  const now = Date.now();
  const c = { ...(mine.c || {}), [k]: ((mine.c?.[k] || 0) + 1) };
  const h = [ ...(mine.h || []), now ].slice(-6);
  const data = { ...mine, c, h, last: { k, t: now } };

  players[meId] = data;
  lastK = k;
  publishPlayer(currentEventId, meId, data);
  renderEvent();

  if (it.points === 0) confetti(['💧']);
  else if (it.points >= 2) confetti([it.emoji || '🥂','🎉']);

  toast(`+${it.points} ${it.emoji || ''} ${it.name}`);
}

function undo(){
  if (!ready || !client?.connected) return toast('Sin conexión');
  const mine = players[meId];
  if (!lastK || !mine?.c?.[lastK]) return toast('Nada que deshacer');

  const c = { ...mine.c, [lastK]: mine.c[lastK] - 1 };
  const h = (mine.h || []).slice(0, -1);
  const data = { ...mine, c, h };
  players[meId] = data;
  publishPlayer(currentEventId, meId, data);
  lastK = null;
  renderEvent();
  toast('Deshecho');
}

function joinEvent(){
  if (!ready || !client?.connected) return toast('Sin conexión');
  const name = $('nick').value.trim();
  if (!name){ $('nick').focus(); return; }

  const existing = players[meId] || {};
  const data = {
    name, av: avatar,
    c: existing.c || {},
    h: existing.h || [],
    last: existing.last || { k:null, t:Date.now() }
  };
  players[meId] = data;
  publishPlayer(currentEventId, meId, data);
  renderEvent();
  toast(`¡Dentro, ${name}!`);
}

/* ==============================================================
   ACCIONES ADMIN
   ============================================================== */
function tryBecomeAdmin(){
  const pwd = prompt('Contraseña de admin:');
  if (pwd === null) return;
  if (pwd === ADMIN_PASSWORD){
    isAdmin = true;
    try { localStorage.setItem('cc_admin', 'yes'); } catch(e){}
    render();
    toast('Modo admin activado');
  } else {
    toast('Contraseña incorrecta');
  }
}

function logoutAdmin(){
  if (!confirm('¿Salir de modo admin?')) return;
  isAdmin = false;
  try { localStorage.removeItem('cc_admin'); } catch(e){}
  render();
}

function createEvent(){
  if (!isAdmin || !ready || !client?.connected) return;
  const name = prompt('Nombre del evento:');
  if (!name || !name.trim()) return;

  const id = 'e' + Date.now().toString(36) + Math.random().toString(36).slice(2,5);
  events[id] = { name: name.trim(), status:'open', createdAt: Date.now() };

  client.publish(EVENTS_TOPIC, JSON.stringify(events), { qos:0, retain:true });
  client.publish(ITEMS_TOPIC(id), JSON.stringify(DEFAULT_ITEMS), { qos:0, retain:true });

  toast('Evento creado');
  render();
}

function addItem(){
  if (!isAdmin || !ready || !client?.connected) return toast('Sin conexión');
  const emoji  = ($('new-item-emoji').value.trim() || '🍹').slice(0, 4);
  const name   = $('new-item-name').value.trim();
  const points = Number($('new-item-points').value);
  if (!name) return toast('Pon un nombre');
  if (!Number.isFinite(points) || points < 0) return toast('Puntos inválidos');

  const id = 'i' + Date.now().toString(36) + Math.random().toString(36).slice(2,4);
  items[id] = { emoji, name, points };
  publishItems();
  renderEvent();
  toast(`${emoji} ${name} añadido (+${points})`);

  $('new-item-name').value = '';
  $('new-item-points').value = '1';
  $('new-item-emoji').value = '🍹';
}

function deleteItem(id){
  if (!isAdmin || !items[id]) return;
  if (!confirm(`¿Borrar "${items[id].name}"?`)) return;
  delete items[id];
  publishItems();
  renderEvent();
}

function finishEvent(){
  if (!isAdmin) return;
  const ev = events[currentEventId];
  if (!ev || ev.status === 'finished') return;
  if (!confirm('¿Finalizar el evento? Ya no se podrán sumar más puntos.')) return;

  events[currentEventId] = { ...ev, status:'finished', finishedAt: Date.now() };
  publishEvents();
  renderEvent();
  toast('Evento finalizado');
}

/* ==============================================================
   LISTENERS
   ============================================================== */
$('btn-admin-login').addEventListener('click', tryBecomeAdmin);
$('btn-admin-logout').addEventListener('click', logoutAdmin);
$('btn-new-event').addEventListener('click', createEvent);
$('btn-back').addEventListener('click', backToList);
$('joinBtn').addEventListener('click', joinEvent);
$('undo').addEventListener('click', undo);
$('btn-add-item').addEventListener('click', addItem);
$('btn-finish-event').addEventListener('click', finishEvent);

/* Eventos de red del navegador */
window.addEventListener('online', () => {
  if (!ready) connectMqtt();
});
window.addEventListener('offline', () => {
  connected = false; updateOverlay();
});

/* ==============================================================
   BOOTSTRAP
   ============================================================== */
(function init(){
  try {
    meId = localStorage.getItem('cc_me_id');
    if (!meId){
      meId = 'p' + Math.random().toString(36).slice(2,10);
      localStorage.setItem('cc_me_id', meId);
    }
    const savedAv = localStorage.getItem('cc_avatar');
    avatar = (savedAv && AVS.includes(savedAv))
      ? savedAv
      : AVS[Math.floor(Math.random()*AVS.length)];
    isAdmin = localStorage.getItem('cc_admin') === 'yes';
  } catch(e){}

  renderAvatarPicker();
  updateOverlay();
  connectMqtt();
})();
