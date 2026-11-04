/* ==============================================================
   Cuenta Cubatas v3 — Firebase Realtime Database
   ============================================================== */

/* ---------------- CONFIG: PON AQUÍ LA TUYA ---------------- */
// Import the functions you need from the SDKs you need
// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyC5sqPWQc79llmtiPCtOt-sE2HReGHkB2A",
  authDomain: "cuenta-cubatas.firebaseapp.com",
  databaseURL: "https://cuenta-cubatas-default-rtdb.europe-west1.firebasedatabase.app",
  projectId: "cuenta-cubatas",
  storageBucket: "cuenta-cubatas.firebasestorage.app",
  messagingSenderId: "219242174502",
  appId: "1:219242174502:web:ca219fb225f0d32d8caee7"
};

/* Solo estos correos son admin (login con Google) */
const ADMIN_EMAILS = ["apm2597@gmail.com", "dejuanaguadomanuel@gmail.com"];

/* ---------------- IMPORTS ---------------- */
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  getAuth, signInAnonymously, onAuthStateChanged,
  GoogleAuthProvider, signInWithPopup, signOut
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import {
  getDatabase, ref, onValue, set, update, push, remove,
  serverTimestamp, increment
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-database.js";

const app  = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db   = getDatabase(app);

/* ---------------- ESTADO ---------------- */
const AVS = ['🦁','🐯','🦊','🐺','🐸','🦄','🐙','🦖','🐧','🤠','😎','🥳','🐉','🐨','🐮','🦉'];

const DEFAULT_ITEMS = {
  cana:     { emoji:'🍺', name:'Caña',     points:1 },
  rebujito: { emoji:'🌸', name:'Rebujito', points:1 },
  vino:     { emoji:'🍷', name:'Vino',     points:1 },
  copa:     { emoji:'🍸', name:'Copazo',   points:2 },
  chupito:  { emoji:'🥃', name:'Chupito',  points:1 },
  agua:     { emoji:'💧', name:'Agua',     points:0 }
};

const state = {
  user: null, isAdmin: false,
  view: 'list',
  eventId: null, evento: null,
  eventos: {}
};

let meId = null;
let avatar = AVS[0];
let detachEvento = null;
let busy = false;
let lastK = null;

const $   = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

/* ---------------- TOAST / OVERLAY ---------------- */
let toastTimer;
function toast(msg){
  const t = $('toast'); if (!t) return;
  t.textContent = msg; t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 2200);
}

function showOverlay(text, opts = {}){
  $('overlay').classList.remove('hide');
  $('app').classList.add('hide');
  $('overlay-text').textContent = text;
  $('overlay-spinner').classList.toggle('hide', opts.spinner === false);
  $('overlay-retry').classList.toggle('hide', !opts.retry);
}
function hideOverlay(){
  $('overlay').classList.add('hide');
  $('app').classList.remove('hide');
}

/* ---------------- HELPERS ---------------- */
const fmtDate = ts => ts
  ? new Date(ts).toLocaleDateString('es-ES', { day:'2-digit', month:'short', year:'numeric' })
  : '';

function guard(){
  if (!state.user){ toast('Conectando…'); return false; }
  return true;
}

/* ---------------- AUTH ---------------- */
onAuthStateChanged(auth, (user) => {
  if (!user){
    signInAnonymously(auth).catch(err => {
      console.error(err);
      showOverlay('Error de autenticación. Revisa la configuración de Firebase.', { spinner:false, retry:true });
      $('overlay-retry').onclick = () => location.reload();
    });
    return;
  }
  state.user = user;
  state.isAdmin = !!user.email && ADMIN_EMAILS.includes(user.email.toLowerCase());
  meId = user.uid;
  hideOverlay();
  render();
});

async function loginAdmin(){
  if (!guard()) return;
  try {
    await signInWithPopup(auth, new GoogleAuthProvider());
  } catch(e){
    console.error(e);
    toast('No se pudo iniciar sesión como admin');
  }
}
async function logoutAdmin(){
  if (!confirm('¿Salir del modo admin?')) return;
  await signOut(auth); // vuelve a anónimo automáticamente
}

/* ---------------- SUSCRIPCIÓN GLOBAL ---------------- */
onValue(ref(db, 'eventos'), (snap) => {
  state.eventos = snap.val() || {};
  if (state.view === 'list') render();
});

/* ---------------- RENDER ---------------- */
function render(){
  if (!state.user) return;
  if (state.view === 'list') renderList();
  else renderEvent();
}

/* ---------- Vista LISTA ---------- */
function renderList(){
  $('view-list').classList.remove('hide');
  $('view-event').classList.add('hide');

  $('btn-admin-login').classList.toggle('hide', state.isAdmin);
  $('btn-admin-logout').classList.toggle('hide', !state.isAdmin);
  $('btn-new-event').classList.toggle('hide', !state.isAdmin);
  $('admin-state').textContent = state.isAdmin ? '✅ Modo admin' : 'Modo jugador';

  const evs = Object.entries(state.eventos)
    .map(([id, e]) => ({ id, ...e }))
    .sort((a,b) => {
      const aO = a.estado !== 'finalizado';
      const bO = b.estado !== 'finalizado';
      if (aO !== bO) return aO ? -1 : 1;
      return (b.creadoEn || 0) - (a.creadoEn || 0);
    });

  $('events-list').innerHTML = evs.length
    ? evs.map(e => {
        const fin = e.estado === 'finalizado';
        return `<button class="event" data-open="${e.id}">
          <div>
            <div class="event-name">${esc(e.nombre || 'Evento')}</div>
            <div class="sub" style="margin:2px 0 0;text-align:left">${fmtDate(e.creadoEn)}</div>
          </div>
          <span class="badge ${fin ? 'bad' : 'ok'}">${fin ? 'Finalizado' : 'Abierto'}</span>
        </button>`;
      }).join('')
    : `<div class="card" style="text-align:center;color:var(--mut)">
         ${state.isAdmin ? 'Pulsa "+ Nuevo evento" para empezar.' : 'Todavía no hay eventos.'}
       </div>`;

  document.querySelectorAll('[data-open]').forEach(b =>
    b.addEventListener('click', () => openEvent(b.dataset.open)));
}

/* ---------- Vista EVENTO ---------- */
function renderEvent(){
  $('view-list').classList.add('hide');
  $('view-event').classList.remove('hide');

  const e = state.evento;
  if (!e){ $('ev-name').textContent = 'Cargando…'; return; }

  const isOpen = e.estado !== 'finalizado';
  const mine = e.jugadores?.[meId];
  const items = e.items || {};

  $('ev-name').textContent = e.nombre || 'Evento';
  $('ev-status').innerHTML = `<span class="badge ${isOpen ? 'ok' : 'bad'}">
    ${isOpen ? 'Abierto' : 'Finalizado'}</span>`;

  $('ev-join').classList.toggle('hide', !!mine || !isOpen);
  $('ev-content').classList.toggle('hide', !mine);
  if (!mine){ renderAvatarPicker(); return; }

  $('meAv').textContent = mine.av || '🍹';
  $('meNm').textContent = mine.nombre;
  $('mePts').textContent = mine.puntos || 0;

  const list = Object.entries(e.jugadores || {})
    .map(([id, j]) => ({ id, ...j }))
    .sort((a,b) => (b.puntos || 0) - (a.puntos || 0));
  const rank = list.findIndex(p => p.id === meId) + 1;
  const totalDrinks = Object.values(mine.consumos || {}).reduce((s,n)=>s+n, 0);
  $('meSt').textContent = `Nº ${rank} de ${list.length} · total ${totalDrinks} bebidas`;

  const keys = Object.keys(items);
  $('btns').innerHTML = keys.length
    ? keys.map(id => {
        const it = items[id];
        return `<button class="dk" data-k="${id}" ${!isOpen ? 'disabled' : ''}>
          <b>${it.emoji || '🍹'}</b>${esc(it.name)}<br>
          <small>${it.points ? '+'+it.points : 'bonus'}</small>
        </button>`;
      }).join('')
    : `<div class="sub" style="grid-column:1/-1">
         ${state.isAdmin ? 'Añade ítems en el panel ↓' : 'El admin aún no ha añadido bebidas.'}
       </div>`;

  $('warn').innerHTML = (mine.racha >= 3 && isOpen)
    ? '<div class="warn">🔥 Racha: 3+ en 20 min. ¡A por la corona! 👑</div>' : '';

  renderRanking(list);
  renderAdminPanel(isOpen, items);

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
    const b = e.target.closest('[data-av]'); if (!b) return;
    avatar = b.dataset.av;
    try { localStorage.setItem('cc_avatar', avatar); } catch(err){}
    box.querySelectorAll('.av').forEach(x => x.classList.toggle('on', x === b));
  };
}

function renderRanking(list){
  if (!list.length){ $('pod').innerHTML = ''; $('rows').innerHTML = ''; return; }
  const max = Math.max(1, ...list.map(p => p.puntos || 0));

  const top = [list[1], list[0], list[2]], hs = [95,125,75];
  $('pod').innerHTML = top.map((p,i) => !p ? '<div class="pc"></div>' : `
    <div class="pc">
      <div class="em">${p.av || '🍹'}</div>
      <div class="nm">${esc(p.nombre)}</div>
      <div class="bar" style="height:${hs[i]*(0.35+0.65*(p.puntos||0)/max)}px">
        ${['🥈','🥇','🥉'][i]} ${p.puntos || 0}
      </div>
    </div>`).join('');

  $('rows').innerHTML = list.map((p,i) => `
    <div class="row">
      <div class="rk">${i+1}</div>
      <div class="em">${p.av || '🍹'}</div>
      <div class="in">
        <div class="nm">${esc(p.nombre)}${p.id===meId?' (tú)':''}
          ${p.id === list[0].id && (p.puntos||0) > 0 ? '<span class="bd">👑 líder</span>' : ''}
        </div>
        <div class="tr"><i style="width:${(p.puntos||0)/max*100}%"></i></div>
      </div>
      <div class="sc">${p.puntos || 0}</div>
    </div>`).join('');
}

function renderAdminPanel(isOpen, items){
  $('admin-panel').classList.toggle('hide', !state.isAdmin);
  if (!state.isAdmin) return;

  $('items-manage').innerHTML = Object.entries(items).map(([id, it]) => `
    <div class="row" style="padding:6px 0">
      <span class="em">${it.emoji || '🍹'}</span>
      <div class="in"><div class="nm">${esc(it.name)} <span class="bd">+${it.points}</span></div></div>
      <button class="sm" data-del-item="${id}" style="color:var(--rose)">Borrar</button>
    </div>`).join('') || `<div class="sub" style="margin:0;text-align:left">Sin ítems</div>`;

  document.querySelectorAll('[data-del-item]').forEach(b =>
    b.addEventListener('click', () => deleteItem(b.dataset.delItem)));

  $('btn-finish-event').classList.toggle('hide', !isOpen);
}

function updateFeed(){ /* no usado en v3, reservado */ }

/* ---------------- NAVEGACIÓN ---------------- */
function openEvent(id){
  state.view = 'event';
  state.eventId = id;
  state.evento = null;
  if (detachEvento) detachEvento();
  detachEvento = onValue(ref(db, `eventos/${id}`), (snap) => {
    state.evento = snap.val();
    renderEvent();
  });
}

function backToList(){
  if (detachEvento) detachEvento();
  detachEvento = null;
  state.view = 'list';
  state.eventId = null;
  state.evento = null;
  render();
}

/* ---------------- ACCIONES JUGADOR ---------------- */
async function joinEvent(){
  if (!guard() || busy) return;
  const name = $('nick').value.trim();
  if (!name) return toast('Escribe tu nombre');
  busy = true;
  try {
    await set(ref(db, `eventos/${state.eventId}/jugadores/${meId}`), {
      nombre: name,
      av: avatar,
      puntos: 0,
      consumos: {},
      racha: 0,
      creadoEn: Date.now()
    });
  } catch(e){
    console.error(e); toast('Error al unirse');
  } finally { busy = false; }
}

async function drink(k){
  if (!guard() || busy) return;
  const e = state.evento;
  if (!e || e.estado === 'finalizado') return;
  const item = e.items?.[k];
  const mine = e.jugadores?.[meId];
  if (!item) return;
  if (!mine) return toast('Únete primero');

  busy = true;
  try {
    // consumos: contador por tipo (para deshacer)
    const prev = mine.consumos?.[k] || 0;
    await update(ref(db, `eventos/${state.eventId}/jugadores/${meId}`), {
      [`consumos/${k}`]: prev + 1,
      puntos: increment(item.points || 0),
      ultimo: { k, t: Date.now() }
    });
    lastK = k;
    toast(`+${item.points} ${item.emoji || ''} ${item.name}`);
  } catch(e){
    console.error(e); toast('Error al sumar');
  } finally { busy = false; }
}

async function undo(){
  if (!guard() || busy || !lastK) return toast('Nada que deshacer');
  const e = state.evento;
  const mine = e?.jugadores?.[meId];
  const item = e?.items?.[lastK];
  if (!mine || !item) return;
  const prev = mine.consumos?.[lastK] || 0;
  if (prev <= 0) return toast('Nada que deshacer');

  busy = true;
  try {
    await update(ref(db, `eventos/${state.eventId}/jugadores/${meId}`), {
      [`consumos/${lastK}`]: prev - 1,
      puntos: increment(-(item.points || 0))
    });
    lastK = null;
    toast('Deshecho');
  } finally { busy = false; }
}

/* ---------------- ACCIONES ADMIN ---------------- */
async function createEvent(){
  if (!state.isAdmin || busy) return;
  const nombre = prompt('Nombre del evento:');
  if (!nombre || !nombre.trim()) return;

  busy = true;
  try {
    const refNew = push(ref(db, 'eventos'));
    await set(refNew, {
      nombre: nombre.trim(),
      estado: 'abierto',
      creadoPor: state.user.uid,
      creadoEn: Date.now(),
      items: DEFAULT_ITEMS
    });
    toast('Evento creado');
  } finally { busy = false; }
}

async function addItem(){
  if (!state.isAdmin || busy) return;
  const emoji  = ($('new-item-emoji').value.trim() || '🍹').slice(0, 4);
  const nombre = $('new-item-name').value.trim();
  const puntos = Number($('new-item-points').value);
  if (!nombre) return toast('Pon un nombre');
  if (!Number.isFinite(puntos) || puntos < 0) return toast('Puntos inválidos');

  busy = true;
  try {
    await push(ref(db, `eventos/${state.eventId}/items`), { emoji, name: nombre, points: puntos });
    toast(`${emoji} ${nombre} añadido (+${puntos})`);
    $('new-item-name').value = '';
    $('new-item-points').value = '1';
    $('new-item-emoji').value = '🍹';
  } finally { busy = false; }
}

async function deleteItem(id){
  if (!state.isAdmin) return;
  if (!confirm('¿Borrar este ítem?')) return;
  await remove(ref(db, `eventos/${state.eventId}/items/${id}`));
}

async function finishEvent(){
  if (!state.isAdmin) return;
  if (!confirm('¿Finalizar el evento? Ya no se podrán sumar más puntos.')) return;
  await update(ref(db, `eventos/${state.eventId}`), {
    estado: 'finalizado',
    finalizadoEn: serverTimestamp()
  });
  toast('Evento finalizado');
}

/* ---------------- LISTENERS ---------------- */
function wire(){
  $('btn-admin-login').addEventListener('click', loginAdmin);
  $('btn-admin-logout').addEventListener('click', logoutAdmin);
  $('btn-new-event').addEventListener('click', createEvent);
  $('btn-back').addEventListener('click', backToList);
  $('joinBtn').addEventListener('click', joinEvent);
  $('undo').addEventListener('click', undo);
  $('btn-add-item').addEventListener('click', addItem);
  $('btn-finish-event').addEventListener('click', finishEvent);
}

/* ---------------- BOOT ---------------- */
(function init(){
  try {
    const sv = localStorage.getItem('cc_avatar');
    avatar = (sv && AVS.includes(sv)) ? sv : AVS[Math.floor(Math.random()*AVS.length)];
  } catch(e){}

  wire();
  showOverlay('Conectando…');
})();