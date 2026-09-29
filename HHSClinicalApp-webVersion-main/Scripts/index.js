/* ---------------- IndexedDB layer ---------------- */
const DB_NAME = "clinicalProtocolHub";
const STORE_PROTOCOLS = "protocols";
const STORE_CATEGORIES = "categories";
const UNCATEGORIZED_ID = "uncategorized";

let dbPromise = null;
/* ===== CLOUD SYNC START ===== */
/* ---------------- Cloud sync (Firestore, PDFs embedded as data URLs) ----------------
 * Data model under clinicalProtocolHubs/default:
 *   protocols/{id}        protocol metadata (no PDF bytes, no extracted text)
 *   categories/{id}       category name
 *   treatmentTexts/{tid~n}  extracted PDF text, chunked (search index)
 *   pdfChunks/{tid~n}     the PDF itself as a base64 data URL, split into
 *                         <1 MiB pieces (same "data URL" format the HHS app
 *                         keeps in trial.pdfFiles[].data)
 * Cloud Storage is NOT used unless USE_CLOUD_STORAGE is switched on (it needs
 * the paid Blaze plan).
 * Every device keeps a full local copy (IndexedDB) and reconciles with the
 * cloud on startup and whenever the cloud changes.
 */
const USE_CLOUD_STORAGE = false;      // PDFs are embedded in Firestore; set true only on the Blaze plan
const CLOUD_HUB_ID = "default";
const CLOUD_HUB_COLLECTION = "clinicalProtocolHubs";
const COL_TEXTS = "treatmentTexts";
const COL_PDF_CHUNKS = "pdfChunks";
const COL_SETTINGS = "settings";
const BRANDING_DOC_ID = "branding";
const TEXT_CHUNK_CHARS = 150000;      // keeps each doc well under Firestore's 1 MiB limit
const PDF_CHUNK_CHARS = 700000;       // base64 chars per doc
const PDF_CHUNKS_PER_BATCH = 4;
const FIREBASE_SDK_VERSION = "10.12.2";
const FIRST_SYNC_TIMEOUT_MS = 8000;
const SYNCED_PROTOCOLS_KEY = "hhsCloudSyncedProtocolIds";
const SYNCED_CATEGORIES_KEY = "hhsCloudSyncedCategoryIds";
const PENDING_DELETES_KEY = "hhsCloudPendingDeletes";
const FILE_FIELDS = ["fileUrl","fileStore","filePartCount","fileSig","textSig"];   // cloud-side bookkeeping on each treatment

/* Same public web config as firebaseconfig.js. Used only if that file did not
 * load (wrong path / script order in index.html). Not a secret - your
 * Firestore/Storage rules are what protect the data. */
const FIREBASE_FALLBACK_CONFIG = {
  apiKey: "AIzaSyBcpNHi6qszWX1IKnl6DKsIOz5R_vu3kMg",
  authDomain: "hhsclinical.firebaseapp.com",
  projectId: "hhsclinical",
  storageBucket: "hhsclinical.firebasestorage.app",
  messagingSenderId: "959109484483",
  appId: "1:959109484483:web:bd8a7fafe6f154e902f776"
};

let cloudDb = null;
let cloudStorage = null;
let cloudReady = false;
let storageUnavailable = false;
let storageFallbackNotified = false;
let pdfProblem = "";                  // last PDF upload/download error, shown in the status line
const pdfUploadAttempts = new Map();  // protocolId -> tries this session
let cloudStatus = { state:"off", detail:"" };
let cloudProtocolDocs = null;         // Map id -> data, null until first server snapshot
let cloudCategoryDocs = null;
let cloudTextChunks = new Map();      // treatmentId -> [chunk docs]
let cloudBrandingLoaded = false;
const cloudServerSeen = { protocols:false, categories:false };
let cloudWriteQueue = Promise.resolve();
let cloudCycleChain = Promise.resolve();
let cloudCycleQueued = false;
let cloudFirstApplyDone = false;
let resolveFirstCycle = () => {};
const cloudInflight = new Map();      // "store:id" -> count of queued writes/deletes
let lastCloudToast = { text:"", at:0 };

function loadIdSet(key){
  try{ return new Set(JSON.parse(localStorage.getItem(key)) || []); } catch{ return new Set(); }
}
function saveIdSet(key, set){
  try{ localStorage.setItem(key, JSON.stringify([...set])); } catch{ /* storage unavailable */ }
}
let syncedProtocolIds = loadIdSet(SYNCED_PROTOCOLS_KEY);   // ids we've seen in the cloud
let syncedCategoryIds = loadIdSet(SYNCED_CATEGORIES_KEY);
let pendingDeletes = loadIdSet(PENDING_DELETES_KEY);       // "store:id" deletes not yet confirmed

function cloudHub(){
  return cloudDb.collection(CLOUD_HUB_COLLECTION).doc(CLOUD_HUB_ID);
}
function isDataUrl(value){
  return typeof value === "string" && value.startsWith("data:");
}
function blobToDataUrl(blob){
  return new Promise((resolve,reject)=>{
    const reader = new FileReader();
    reader.onload = ()=> resolve(reader.result);
    reader.onerror = ()=> reject(reader.error);
    reader.readAsDataURL(blob);
  });
}
function plain(obj){ return JSON.parse(JSON.stringify(obj)); }   // drops undefined (Firestore rejects it)

/* Cheap content signature: length + FNV-1a hash. Used to know whether a PDF's
 * text/bytes changed since they were last uploaded. */
function stringSig(s){
  let h = 0x811c9dc5;
  for(let i=0; i<s.length; i++){
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return `${s.length}:${(h >>> 0).toString(16)}`;
}
function chunkId(id, i){ return `${id}~${String(i).padStart(4,"0")}`; }
function codeNameKey(p){ return `${p.code}|${p.name}`; }
function isPristineSeed(p){
  return (p.treatmentOptions || []).length === 0
      && SEED_PROTOCOLS.some(s => s.code === p.code && s.name === p.name);
}

/* ---------- status + error reporting ---------- */
function describeCloudError(err){
  const code = (err && err.code) || "";
  const msg = (err && err.message) || String(err);
  if(code === "permission-denied") return "Firestore rules are blocking access (permission-denied) - publish rules that allow read/write on clinicalProtocolHubs";
  if(code === "unavailable") return "can't reach Firebase (offline, or the network blocks it)";
  if(code === "not-found" || /does not exist/i.test(msg)) return "the Firestore database doesn't exist yet - create it in the Firebase console";
  if(code === "resource-exhausted") return "Firebase free-tier quota reached for today";
  if(code === "storage/unauthorized") return "Cloud Storage rules block uploads";
  if(String(code).startsWith("storage/")) return `Cloud Storage error (${code})`;
  return code ? `${code}: ${msg}` : msg;
}
function notifyCloudProblem(text){
  const now = Date.now();
  if(text === lastCloudToast.text && now - lastCloudToast.at < 60000) return;
  lastCloudToast = { text, at: now };
  showToast(text, { type:"warning", duration:7000 });
}
function setCloudStatus(state, detail){
  cloudStatus = { state, detail: detail || "" };
  renderStorageSummary();
}
function setPdfProblem(text){
  if(pdfProblem === text) return;
  pdfProblem = text;
  renderStorageSummary();
}
function pdfSyncCounts(){
  let up = 0, down = 0;
  for(const p of allProtocols){
    for(const t of (p.treatmentOptions || [])){
      if(treatmentNeedsUpload(t)) up++;
      else if(t.fileName && (t.fileUrl || t.filePartCount) && !isDataUrl(t.fileData)) down++;
    }
  }
  return { up, down };
}
function cloudStatusText(){
  switch(cloudStatus.state){
    case "on": {
      if(pdfProblem) return ` · ☁ Synced, but PDFs have a problem: ${pdfProblem}`;
      const { up, down } = pdfSyncCounts();
      if(up) return ` · ☁ Synced · ⬆ ${up} PDF${up===1?"":"s"} uploading`;
      if(down) return ` · ☁ Synced · ⬇ ${down} PDF${down===1?"":"s"} downloading`;
      return " · ☁ Synced";
    }
    case "connecting": return " · ☁ Connecting…";
    case "error": return ` · ☁ Sync problem: ${cloudStatus.detail}`;
    default: return cloudStatus.detail ? ` · ☁ Sync off: ${cloudStatus.detail}` : "";
  }
}

/* ---------- Firebase SDK bootstrap ---------- */
function loadScript(src){
  return new Promise((resolve,reject)=>{
    const s = document.createElement("script");
    s.src = src;
    s.onload = ()=> resolve();
    s.onerror = ()=> reject(new Error(`Could not load ${src}`));
    document.head.appendChild(s);
  });
}
async function ensureFirebaseSdk(){
  const base = `https://www.gstatic.com/firebasejs/${FIREBASE_SDK_VERSION}/`;
  if(!window.firebase || !firebase.initializeApp) await loadScript(base + "firebase-app-compat.js");
  if(!firebase.firestore) await loadScript(base + "firebase-firestore-compat.js");
  if(USE_CLOUD_STORAGE && !firebase.storage){
    try{ await loadScript(base + "firebase-storage-compat.js"); }
    catch(err){ console.warn("Firebase Storage SDK not loaded; PDFs will use Firestore:", err); }
  }
  if(!firebase.firestore) throw new Error("Firestore SDK did not load");
}

/* Resolves true once the first cloud<->local reconcile finished, false if the
 * cloud is disabled/unreachable (the app then simply keeps working locally). */
function initializeCloudSync(){
  const firstCycle = new Promise(resolve => { resolveFirstCycle = resolve; });
  startCloudSync()
    .then(started => { if(!started) resolveFirstCycle(false); })
    .catch(err => {
      console.error("Cloud sync failed to start:", err);
      setCloudStatus("off", describeCloudError(err));
      notifyCloudProblem(`Cloud sync is off: ${describeCloudError(err)}`);
      resolveFirstCycle(false);
    });
  const timeout = new Promise(resolve => setTimeout(()=> resolve(false), FIRST_SYNC_TIMEOUT_MS));
  return Promise.race([firstCycle, timeout]);
}

async function startCloudSync(){
  const config = window.FIREBASE_CONFIG || FIREBASE_FALLBACK_CONFIG;
  if(!config || !config.projectId || /YOUR_|REPLACE|PLACEHOLDER/i.test(config.apiKey || "")){
    setCloudStatus("off", "Firebase config is missing or still has placeholder values");
    notifyCloudProblem("Cloud sync is off: Firebase config is missing.");
    return false;
  }
  setCloudStatus("connecting");
  await ensureFirebaseSdk();
  if(!firebase.apps.length) firebase.initializeApp(config);
  cloudDb = firebase.firestore();
  try{
    // Hospital/guest networks often break Firestore's streaming transport;
    // this lets the SDK fall back to long-polling automatically.
    cloudDb.settings({ experimentalAutoDetectLongPolling:true, merge:true });
  } catch(err){ console.warn("Could not change Firestore settings:", err); }
  if(USE_CLOUD_STORAGE){
    try{ cloudStorage = firebase.storage ? firebase.storage() : null; }
    catch(err){ cloudStorage = null; console.warn("Cloud Storage unavailable:", err); }
  }
  attachCloudListeners();
  return true;
}

function handleCloudListenerError(label, err){
  console.error(`Cloud listener (${label}) failed:`, err);
  const detail = describeCloudError(err);
  setCloudStatus("error", detail);
  notifyCloudProblem(`Cloud sync problem: ${detail}`);
  resolveFirstCycle(false);
}

function attachCloudListeners(){
  const hub = cloudHub();
  // A cache-only snapshot (device offline) says nothing about the cloud, so
  // it must never be treated as "the cloud is empty".
  const authoritative = (snap, key) => {
    if(!snap.metadata.fromCache) cloudServerSeen[key] = true;
    if(snap.metadata.fromCache && !cloudServerSeen[key]){
      setCloudStatus("connecting");
      return false;
    }
    return true;
  };
  hub.collection(STORE_PROTOCOLS).onSnapshot(snap => {
    if(!authoritative(snap, "protocols")) return;
    cloudProtocolDocs = new Map(snap.docs.map(d => [d.id, d.data()]));
    scheduleCloudCycle();
  }, err => handleCloudListenerError("protocols", err));
  hub.collection(STORE_CATEGORIES).onSnapshot(snap => {
    if(!authoritative(snap, "categories")) return;
    cloudCategoryDocs = new Map(snap.docs.map(d => [d.id, d.data()]));
    scheduleCloudCycle();
  }, err => handleCloudListenerError("categories", err));
  hub.collection(COL_TEXTS).onSnapshot(snap => {
    if(snap.metadata.fromCache && !cloudServerSeen.protocols) return;
    const map = new Map();
    for(const d of snap.docs){
      const x = d.data();
      if(!map.has(x.treatmentId)) map.set(x.treatmentId, []);
      map.get(x.treatmentId).push(x);
    }
    cloudTextChunks = map;
    scheduleCloudCycle();
  }, err => console.warn("Text index listener failed:", err));
  hub.collection(COL_SETTINGS).doc(BRANDING_DOC_ID).onSnapshot(snap => {
    if(snap.metadata.fromCache && !cloudBrandingLoaded) return;
    cloudBrandingLoaded = true;
    if(snap.exists){
      applyCloudBranding(snap.data());
    } else {
      syncBrandingToCloud();
    }
  }, err => console.warn("Branding listener failed:", err));
}

function scheduleCloudCycle(){
  if(cloudCycleQueued) return;
  cloudCycleQueued = true;
  cloudCycleChain = cloudCycleChain.then(async ()=>{
    cloudCycleQueued = false;
    await runCloudCycle();
  }).catch(err => {
    console.error("Cloud sync cycle failed:", err);
    setCloudStatus("error", describeCloudError(err));
  });
}

async function runCloudCycle(){
  if(!cloudProtocolDocs || !cloudCategoryDocs) return;
  if(!cloudReady){
    cloudReady = true;
    reconcileLocalWithCloud();
  }
  if(cloudStatus.state !== "on") setCloudStatus("on");
  await applyCloudState();
  resolveFirstCycle(true);
  ensurePendingUploads();
  hydratePdfs().catch(err => console.warn("PDF download pass failed:", err));
}

/* ---------- record <-> cloud shape ---------- */
function cloudProtocol(protocol){
  return plain({
    ...protocol,
    treatmentOptions: (protocol.treatmentOptions || []).map(t => {
      const copy = { ...t };
      delete copy.fileData;     // PDF bytes live in Storage / pdfChunks
      delete copy.fileBlob;
      delete copy.fileText;     // extracted text lives in treatmentTexts
      delete copy.textPending;  // local-only flag
      return copy;
    })
  });
}

function assembleText(t){
  if(!t.textSig) return undefined;
  if(t.textSig.startsWith("0:")) return "";
  const parts = (cloudTextChunks.get(t.id) || []).filter(x => x.sig === t.textSig);
  if(parts.length === 0 || parts.length !== parts[0].n) return undefined;
  return parts.sort((a,b) => a.i - b.i).map(x => x.text).join("");
}

function localProtocolFromCloud(cloud, local){
  const prevById = new Map(((local && local.treatmentOptions) || []).map(t => [t.id, t]));
  return {
    ...cloud,
    treatmentOptions: (cloud.treatmentOptions || []).map(t => {
      const prev = prevById.get(t.id);
      const out = { ...t };

      // Never drop a PDF this device already holds unless the cloud clearly
      // has a *different* file (another device replaced it). If the cloud has
      // no confirmed upload yet (failed/pending), the local copy is the only one.
      let keepLocal = false;
      if(prev && isDataUrl(prev.fileData)){
        keepLocal = !t.fileSig || (prev.fileSig || stringSig(prev.fileData)) === t.fileSig;
      }
      if(keepLocal) out.fileData = prev.fileData;
      else out.fileData = t.fileUrl || null;      // Storage link works immediately; Firestore PDFs get downloaded

      let text = assembleText(t);
      if(text === undefined && prev && prev.textSig === t.textSig && typeof prev.fileText === "string") text = prev.fileText;
      if(text === undefined && !t.textSig){
        // No text uploaded to the cloud yet: legacy docs kept it inline; otherwise keep whatever this device has.
        text = (typeof t.fileText === "string" && t.fileText) ? t.fileText : ((prev && prev.fileText) || "");
      }
      out.textPending = (text === undefined);
      out.fileText = (text !== undefined) ? text : ((prev && prev.fileText) || "");
      return out;
    })
  };
}

function protocolFingerprint(p){
  return JSON.stringify(cloudProtocol(p)) + "|" + (p.treatmentOptions || []).map(t =>
    (t.fileText || "").length + (isDataUrl(t.fileData) ? "d" : (t.fileData ? "u" : "n"))
  ).join(",");
}

function treatmentNeedsUpload(t){
  return (isDataUrl(t.fileData) && (!t.fileSig || (!t.fileUrl && !t.filePartCount))) ||
         (!t.textPending && !t.textSig && (t.fileText || "").length > 0);
}
function protocolNeedsUpload(p){
  return (p.treatmentOptions || []).some(treatmentNeedsUpload);
}

/* ---------- uploads ---------- */
async function deleteStaleChunks(collectionName, treatmentId, sig, n){
  const snap = await cloudHub().collection(collectionName).where("treatmentId","==",treatmentId).get();
  const stale = snap.docs.filter(d => { const x = d.data(); return x.sig !== sig || x.i >= n; });
  for(let i=0; i<stale.length; i+=400){
    const batch = cloudDb.batch();
    stale.slice(i, i+400).forEach(d => batch.delete(d.ref));
    await batch.commit();
  }
}

async function uploadPdfToFirestore(t, sig, hadChunks){
  const data = t.fileData;
  const n = Math.ceil(data.length / PDF_CHUNK_CHARS);
  const col = cloudHub().collection(COL_PDF_CHUNKS);
  for(let start=0; start<n; start += PDF_CHUNKS_PER_BATCH){
    const batch = cloudDb.batch();
    for(let i=start; i<Math.min(n, start + PDF_CHUNKS_PER_BATCH); i++){
      batch.set(col.doc(chunkId(t.id, i)), {
        treatmentId: t.id, sig, i, n,
        data: data.slice(i * PDF_CHUNK_CHARS, (i+1) * PDF_CHUNK_CHARS)
      });
    }
    await batch.commit();
  }
  if(hadChunks) await deleteStaleChunks(COL_PDF_CHUNKS, t.id, sig, n);
  return n;
}

async function downloadPdfFromFirestore(t){
  const snap = await cloudHub().collection(COL_PDF_CHUNKS).where("treatmentId","==",t.id).get();
  const parts = snap.docs.map(d => d.data()).filter(x => x.sig === t.fileSig).sort((a,b) => a.i - b.i);
  if(parts.length === 0 || parts.length !== parts[0].n) throw new Error("PDF chunks incomplete");
  return parts.map(x => x.data).join("");
}

async function syncTreatmentFile(protocolId, t){
  if(!isDataUrl(t.fileData)) return;                 // nothing local to upload
  const sig = stringSig(t.fileData);
  if(t.fileSig === sig && (t.fileUrl || t.filePartCount)) return;   // already up to date

  if(USE_CLOUD_STORAGE && cloudStorage && !storageUnavailable){
    try{
      const ref = cloudStorage.ref(`clinical-protocols/${CLOUD_HUB_ID}/${protocolId}/${t.id}-${sig.replace(":","-")}.pdf`);
      await ref.put(dataUrlToBlob(t.fileData), { contentType:"application/pdf" });
      t.fileUrl = await ref.getDownloadURL();
      t.fileStore = "storage";
      t.filePartCount = null;
      t.fileSig = sig;
      return;
    } catch(err){
      if(!String((err && err.code) || "").startsWith("storage/")) throw err;
      // Storage needs the Blaze plan (and a bucket + rules). Fall back to Firestore.
      storageUnavailable = true;
      console.warn("Cloud Storage unavailable, storing PDFs in Firestore instead:", err);
      if(!storageFallbackNotified){
        storageFallbackNotified = true;
        showToast("Cloud Storage isn't available on this Firebase plan - storing PDFs in Firestore instead.", { type:"warning", duration:6000 });
      }
      renderStorageSummary();
    }
  }
  const hadChunks = t.fileStore === "firestore" && t.filePartCount > 0;
  const n = await uploadPdfToFirestore(t, sig, hadChunks);
  t.fileUrl = null;
  t.fileStore = "firestore";
  t.filePartCount = n;
  t.fileSig = sig;
}

async function syncTreatmentText(t){
  if(t.textPending) return;               // haven't received the cloud text yet - never overwrite it
  const text = t.fileText || "";
  const sig = stringSig(text);
  if(t.textSig === sig) return;
  const n = Math.ceil(text.length / TEXT_CHUNK_CHARS);
  const col = cloudHub().collection(COL_TEXTS);
  for(let i=0; i<n; i++){
    await col.doc(chunkId(t.id, i)).set({
      treatmentId: t.id, sig, i, n,
      text: text.slice(i * TEXT_CHUNK_CHARS, (i+1) * TEXT_CHUNK_CHARS)
    });
  }
  if(t.textSig) await deleteStaleChunks(COL_TEXTS, t.id, sig, n);
  t.textSig = sig;
}

/* A failed PDF upload must not block the protocol's metadata from syncing,
 * so errors are collected and reported after the document is written. */
async function prepareCloudProtocol(protocol){
  let firstError = null;
  for(const t of (protocol.treatmentOptions || [])){
    try{ await syncTreatmentFile(protocol.id, t); }
    catch(err){ console.error(`PDF upload failed for "${t.fileName || t.name}":`, err); firstError = firstError || err; setPdfProblem(`upload failed - ${describeCloudError(err)}`); }
    try{ await syncTreatmentText(t); }
    catch(err){ console.error(`Text index upload failed for "${t.fileName || t.name}":`, err); firstError = firstError || err; }
  }
  return { prepared: cloudProtocol(protocol), error: firstError };
}

/* A whole-document write from a device that hasn't seen the latest PDF
 * bookkeeping must not wipe it: keep the cloud's file fields for any
 * treatment this device has no file info for. */
function mergeRemoteFileFields(prepared){
  const remote = cloudProtocolDocs && cloudProtocolDocs.get(prepared.id);
  if(!remote) return;
  const byId = new Map((remote.treatmentOptions || []).map(t => [t.id, t]));
  for(const t of (prepared.treatmentOptions || [])){
    const rt = byId.get(t.id);
    if(!rt || t.fileSig || t.fileUrl || t.filePartCount) continue;
    for(const k of FILE_FIELDS) if(rt[k] !== undefined) t[k] = rt[k];
  }
}

/* Uploads PDFs/text this device holds but the cloud doesn't have yet, then
 * patches ONLY those treatments' file fields into the cloud document (a
 * transaction, so it can't clobber other devices' edits). This runs even when
 * another device has since edited the protocol. */
function uploadPendingFiles(protocolId){
  const key = `${STORE_PROTOCOLS}:${protocolId}`;
  markInflight(key);
  return queueCloudWrite(async ()=>{
    const p = allProtocols.find(x => x.id === protocolId);
    if(!p) return;
    const remote = cloudProtocolDocs && cloudProtocolDocs.get(protocolId);
    const remoteById = new Map(((remote && remote.treatmentOptions) || []).map(t => [t.id, t]));
    const targets = (p.treatmentOptions || []).filter(t => {
      const rt = remoteById.get(t.id);
      return treatmentNeedsUpload(t) && !(rt && rt.fileSig && !t.fileSig);   // else another device already uploaded a file
    });
    let firstError = null;
    for(const t of targets){
      try{ await syncTreatmentFile(p.id, t); }
      catch(err){ console.error(`PDF upload failed for "${t.fileName || t.name}":`, err); firstError = firstError || err; }
      try{ await syncTreatmentText(t); }
      catch(err){ console.error(`Text index upload failed for "${t.fileName || t.name}":`, err); firstError = firstError || err; }
    }
    const ids = new Set(targets.map(t => t.id));
    const ref = cloudHub().collection(STORE_PROTOCOLS).doc(p.id);
    await cloudDb.runTransaction(async tx => {
      const snap = await tx.get(ref);
      if(!snap.exists) return;
      const data = snap.data();
      let changed = false;
      for(const ct of (data.treatmentOptions || [])){
        if(!ids.has(ct.id)) continue;
        const lt = p.treatmentOptions.find(x => x.id === ct.id);
        for(const k of FILE_FIELDS){
          if(lt[k] !== undefined && JSON.stringify(ct[k]) !== JSON.stringify(lt[k])){ ct[k] = lt[k]; changed = true; }
        }
      }
      if(changed) tx.set(ref, plain(data));
    });
    const current = allProtocols.find(x => x.id === protocolId);
    if(current){
      if(current !== p) copyCloudFields(p, current);
      await dbPutLocal(STORE_PROTOCOLS, current);
    }
    if(firstError){ setPdfProblem(`upload failed - ${describeCloudError(firstError)}`); throw firstError; }
    pdfUploadAttempts.delete(protocolId);
    if(pdfProblem.startsWith("upload")) setPdfProblem("");
    renderStorageSummary();
  }, key);
}

/* Called after every sync cycle: anything still unsent gets (re)tried, up to
 * 3 times in a row, then at most once a minute, so a permanent error
 * (e.g. blocked by rules) can't loop. */
function ensurePendingUploads(){
  for(const p of allProtocols){
    if(!protocolNeedsUpload(p)) continue;
    if(cloudInflight.has(`${STORE_PROTOCOLS}:${p.id}`)) continue;
    const prev = pdfUploadAttempts.get(p.id) || { n:0, at:0 };
    if(prev.n >= 3 && Date.now() - prev.at < 60000) continue;
    pdfUploadAttempts.set(p.id, { n: prev.n + 1, at: Date.now() });
    uploadPendingFiles(p.id);
  }
}

function copyCloudFields(from, to){
  const src = new Map((from.treatmentOptions || []).map(t => [t.id, t]));
  for(const t of (to.treatmentOptions || [])){
    const f = src.get(t.id);
    if(!f) continue;
    for(const k of FILE_FIELDS){
      if(f[k] !== undefined) t[k] = f[k];
    }
  }
}

/* ---------- write queue ---------- */
function markInflight(key){ cloudInflight.set(key, (cloudInflight.get(key) || 0) + 1); }
function unmarkInflight(key){
  const n = (cloudInflight.get(key) || 1) - 1;
  if(n <= 0) cloudInflight.delete(key); else cloudInflight.set(key, n);
}
function isInflight(key){ return cloudInflight.has(key) || pendingDeletes.has(key); }

function queueCloudWrite(work, key){
  cloudWriteQueue = cloudWriteQueue.then(work).catch(err => {
    console.error("Cloud sync write failed:", err);
    notifyCloudProblem(`Saved on this device, but cloud sync failed: ${describeCloudError(err)}`);
  }).then(()=> { if(key) unmarkInflight(key); });
  return cloudWriteQueue;
}

function syncRecordToCloud(storeName, record){
  const key = `${storeName}:${record.id}`;
  if(pendingDeletes.delete(key)) saveIdSet(PENDING_DELETES_KEY, pendingDeletes);   // e.g. Undo after delete
  if(!cloudReady) return Promise.resolve();
  markInflight(key);
  return queueCloudWrite(async ()=>{
    if(storeName === STORE_PROTOCOLS){
      const { prepared, error } = await prepareCloudProtocol(record);
      mergeRemoteFileFields(prepared);
      await cloudHub().collection(STORE_PROTOCOLS).doc(record.id).set(prepared);
      // loadAll() may have swapped in a fresh copy of this protocol while the
      // upload ran; make sure the copy that's live now gets the upload results.
      const current = allProtocols.find(p => p.id === record.id);
      if(current){
        if(current !== record) copyCloudFields(record, current);
        await dbPutLocal(STORE_PROTOCOLS, current);
      }
      if(error) throw error;
      if(pdfProblem.startsWith("upload")) setPdfProblem("");
      renderStorageSummary();
    } else if(storeName === STORE_CATEGORIES){
      await cloudHub().collection(STORE_CATEGORIES).doc(record.id).set(plain(record));
    }
  }, key);
}

function runCloudDelete(storeName, id){
  const key = `${storeName}:${id}`;
  markInflight(key);
  return queueCloudWrite(async ()=>{
    await cloudHub().collection(storeName).doc(id).delete();
    pendingDeletes.delete(key);
    saveIdSet(PENDING_DELETES_KEY, pendingDeletes);
  }, key);
}

function deleteRecordFromCloud(storeName, id){
  pendingDeletes.add(`${storeName}:${id}`);
  saveIdSet(PENDING_DELETES_KEY, pendingDeletes);   // survives a reload if we're offline right now
  if(!cloudReady) return Promise.resolve();
  return runCloudDelete(storeName, id);
}

/* ---------- reconcile + apply ---------- */
/* Runs once per session, before the first apply: pushes anything this device
 * has that the cloud lacks (first-time migration, offline edits, unsent PDFs). */
function reconcileLocalWithCloud(){
  const remoteKeys = new Set([...cloudProtocolDocs.values()].map(codeNameKey));
  const dropIds = new Set();

  for(const p of allProtocols){
    const remote = cloudProtocolDocs.get(p.id);
    if(!remote){
      if(syncedProtocolIds.has(p.id) || pendingDeletes.has(`${STORE_PROTOCOLS}:${p.id}`)) continue;   // deleted on another device
      // The starter protocols used to get random ids on every device; don't upload duplicates.
      if(isPristineSeed(p) && remoteKeys.has(codeNameKey(p))){ dropIds.add(p.id); continue; }
      syncRecordToCloud(STORE_PROTOCOLS, p);
    } else {
      if((p.updatedAt || "") > (remote.updatedAt || "")) syncRecordToCloud(STORE_PROTOCOLS, p);
    }
  }
  if(dropIds.size){
    allProtocols = allProtocols.filter(p => !dropIds.has(p.id));
    for(const id of dropIds) dbDeleteLocal(STORE_PROTOCOLS, id).catch(()=>{});
  }

  for(const c of allCategories){
    if(cloudCategoryDocs.has(c.id)) continue;
    if(syncedCategoryIds.has(c.id) || pendingDeletes.has(`${STORE_CATEGORIES}:${c.id}`)) continue;
    syncRecordToCloud(STORE_CATEGORIES, c);
  }

  for(const key of [...pendingDeletes]){
    const sep = key.indexOf(":");
    const store = key.slice(0, sep), id = key.slice(sep + 1);
    const docs = store === STORE_PROTOCOLS ? cloudProtocolDocs : cloudCategoryDocs;
    if(docs.has(id)) runCloudDelete(store, id);
    else pendingDeletes.delete(key);
  }
  saveIdSet(PENDING_DELETES_KEY, pendingDeletes);
}

async function applyCloudState(){
  const persist = [];       // [store, record] to write to IndexedDB
  const removeLocal = [];   // [store, id] deleted on another device

  const localP = new Map(allProtocols.map(p => [p.id, p]));
  const nextP = [];
  for(const [id, data] of cloudProtocolDocs){
    const local = localP.get(id);
    const localNewer = local && (local.updatedAt || "") > (data.updatedAt || "");
    if(isInflight(`${STORE_PROTOCOLS}:${id}`) || localNewer){
      if(local) nextP.push(local);       // our own change is on its way to the cloud
      continue;
    }
    const merged = localProtocolFromCloud(data, local);
    normalizeProtocol(merged);
    if(local && protocolFingerprint(local) === protocolFingerprint(merged)) nextP.push(local);
    else { nextP.push(merged); persist.push([STORE_PROTOCOLS, merged]); }
  }
  for(const local of allProtocols){
    if(cloudProtocolDocs.has(local.id)) continue;
    if(!isInflight(`${STORE_PROTOCOLS}:${local.id}`) && syncedProtocolIds.has(local.id)) removeLocal.push([STORE_PROTOCOLS, local.id]);
    else nextP.push(local);              // created here, upload pending
  }

  const localC = new Map(allCategories.map(c => [c.id, c]));
  const nextC = [];
  for(const [id, data] of cloudCategoryDocs){
    const local = localC.get(id);
    if(isInflight(`${STORE_CATEGORIES}:${id}`)){ if(local) nextC.push(local); continue; }
    if(local && local.name === data.name) nextC.push(local);
    else { nextC.push(data); persist.push([STORE_CATEGORIES, data]); }
  }
  for(const local of allCategories){
    if(cloudCategoryDocs.has(local.id)) continue;
    if(!isInflight(`${STORE_CATEGORIES}:${local.id}`) && syncedCategoryIds.has(local.id)) removeLocal.push([STORE_CATEGORIES, local.id]);
    else nextC.push(local);
  }

  syncedProtocolIds = new Set(cloudProtocolDocs.keys());
  syncedCategoryIds = new Set(cloudCategoryDocs.keys());
  saveIdSet(SYNCED_PROTOCOLS_KEY, syncedProtocolIds);
  saveIdSet(SYNCED_CATEGORIES_KEY, syncedCategoryIds);

  // Swap the in-memory lists synchronously (no await in between) so a user
  // edit can't slip through mid-merge; IndexedDB is caught up right after.
  allProtocols = nextP;
  allCategories = nextC;
  if(persist.length || removeLocal.length || !cloudFirstApplyDone){
    cloudFirstApplyDone = true;
    renderChips();
    renderGrid();
    renderRecentChips();
  }
  renderStorageSummary();

  for(const [store, rec] of persist) await dbPutLocal(store, rec);
  for(const [store, id] of removeLocal) await dbDeleteLocal(store, id);
}

/* ---------- PDF download / offline cache ---------- */
const hydrationTried = new Set();
let hydrating = false;

function applyHydratedFile(protocolId, treatmentId, sigAtStart, data){
  const p = allProtocols.find(x => x.id === protocolId);
  const t = p && p.treatmentOptions.find(x => x.id === treatmentId);
  if(!t || isDataUrl(t.fileData) || (t.fileSig || "") !== sigAtStart) return;
  t.fileData = data;
  dbPutLocal(STORE_PROTOCOLS, p).catch(err => console.warn("Could not cache PDF locally:", err));
  renderGrid();
  renderStorageSummary();
}

/* Downloads every PDF this device doesn't have yet and caches it in
 * IndexedDB (so it opens offline). Storage-backed PDFs stay viewable through
 * their link even if the browser blocks the background download (CORS). */
async function hydratePdfs(){
  if(hydrating) return;
  hydrating = true;
  try{
    for(const p of [...allProtocols]){
      for(const t of [...(p.treatmentOptions || [])]){
        if(isDataUrl(t.fileData)) continue;
        const viaFirestore = t.fileStore === "firestore" && t.filePartCount > 0;
        if(!viaFirestore && !t.fileUrl) continue;
        const key = `${t.id}:${t.fileSig || ""}:${t.fileUrl || ""}`;
        if(hydrationTried.has(key)) continue;
        hydrationTried.add(key);
        const sigAtStart = t.fileSig || "";
        let data = null;
        try{
          if(viaFirestore){
            data = await downloadPdfFromFirestore(t);
          } else {
            const response = await fetch(t.fileUrl);
            if(!response.ok) throw new Error(`PDF download failed (${response.status})`);
            data = await blobToDataUrl(await response.blob());
          }
        } catch(err){
          console.warn(`Could not download PDF "${t.fileName || t.name}":`, err);
          if(viaFirestore){
            hydrationTried.delete(key);   // may still be uploading; retry on the next sync event
            if(err && err.code) setPdfProblem(`download failed - ${describeCloudError(err)}`);
          }
        }
        if(data){
          applyHydratedFile(p.id, t.id, sigAtStart, data);
          if(pdfProblem.startsWith("download")) setPdfProblem("");
        }
      }
    }
  } finally {
    hydrating = false;
  }
}
/* ===== CLOUD SYNC END ===== */

function createMissingStores(db){
  if(!db.objectStoreNames.contains(STORE_PROTOCOLS)){
    db.createObjectStore(STORE_PROTOCOLS, { keyPath:"id" });
  }
  if(!db.objectStoreNames.contains(STORE_CATEGORIES)){
    db.createObjectStore(STORE_CATEGORIES, { keyPath:"id" });
  }
}

function rawOpen(version){
  return new Promise((resolve,reject)=>{
    const req = version === undefined
      ? indexedDB.open(DB_NAME)
      : indexedDB.open(DB_NAME, version);
    req.onupgradeneeded = ()=> createMissingStores(req.result);
    req.onsuccess = ()=> resolve(req.result);
    req.onerror = ()=> reject(req.error);
    req.onblocked = ()=> reject(new Error("Database upgrade blocked — close other tabs running this app."));
  });
}

/* Opens without pinning a version number, so an existing database that has
 * already been upgraded past what this script expects still opens cleanly
 * (a hardcoded version lower than the stored one throws VersionError).
 * If the stores we need are missing, reopen at version+1 to create them. */
function openDB(){
  if(dbPromise) return dbPromise;
  dbPromise = (async ()=>{
    let db = await rawOpen();
    const needsStores = !db.objectStoreNames.contains(STORE_PROTOCOLS)
                     || !db.objectStoreNames.contains(STORE_CATEGORIES);
    if(needsStores){
      const nextVersion = db.version + 1;
      db.close();
      db = await rawOpen(nextVersion);
    }
    // Another tab requesting a newer version needs us to get out of the way.
    db.onversionchange = ()=>{ db.close(); dbPromise = null; };
    return db;
  })();
  dbPromise.catch(()=> { dbPromise = null; });
  return dbPromise;
}

async function dbGetAll(storeName){
  const db = await openDB();
  return new Promise((resolve,reject)=>{
    const tx = db.transaction(storeName,"readonly");
    const req = tx.objectStore(storeName).getAll();
    req.onsuccess = ()=> resolve(req.result);
    req.onerror = ()=> reject(req.error);
  });
}

async function dbPutLocal(storeName, record){
  const db = await openDB();
  return new Promise((resolve,reject)=>{
    const tx = db.transaction(storeName,"readwrite");
    tx.objectStore(storeName).put(record);
    tx.oncomplete = ()=> resolve();
    tx.onerror = ()=> reject(tx.error);
  });
}

async function dbPut(storeName, record){
  await dbPutLocal(storeName, record);
  syncRecordToCloud(storeName, record);
}

async function dbPutLocalOnly(storeName, record){
  await dbPutLocal(storeName, record);
}

async function dbDeleteLocal(storeName, id){
  const db = await openDB();
  await new Promise((resolve,reject)=>{
    const tx = db.transaction(storeName,"readwrite");
    tx.objectStore(storeName).delete(id);
    tx.oncomplete = ()=> resolve();
    tx.onerror = ()=> reject(tx.error);
  });
}

async function dbDelete(storeName, id){
  await dbDeleteLocal(storeName, id);
  deleteRecordFromCloud(storeName, id);
}

/* ---------------- Seed data ---------------- */
const SEED_CATEGORIES = [
  { id:"cat-emergency", name:"Emergency" },
  { id:"cat-consent", name:"Consent" },
  { id:"cat-oncology", name:"Oncology" },
  { id:"cat-cardiology", name:"Cardiology" },
  { id:"cat-pediatrics", name:"Pediatrics" },
  { id:"cat-hematology", name:"Hematology" },
  { id:"cat-neurology", name:"Neurology" },
  { id:"cat-surgery", name:"Surgery" },
  { id:"cat-safety", name:"Safety" },
  { id:"cat-screening", name:"Screening" },
  { id: UNCATEGORIZED_ID, name:"Uncategorized" }
];

const SEED_PROTOCOLS = [
  { code:"PROT-001", name:"Sepsis Screening Protocol", categoryIds:["cat-emergency"] },
  { code:"PROT-002", name:"Informed Consent for Trial Enrollment", categoryIds:["cat-consent"] },
  { code:"PROT-003", name:"Chemotherapy Adverse Event Reporting", categoryIds:["cat-oncology"] },
  { code:"PROT-004", name:"Cardiac Biomarker Draw Timing", categoryIds:["cat-cardiology"] },
  { code:"PROT-005", name:"Pediatric Dosage Calculation Checklist", categoryIds:["cat-pediatrics"] },
  { code:"PROT-006", name:"Blood Product Administration", categoryIds:["cat-hematology"] },
  { code:"PROT-007", name:"Stroke Fast-Track Screening", categoryIds:["cat-neurology"] },
  { code:"PROT-008", name:"Post-Operative Infection Monitoring", categoryIds:["cat-surgery"] },
  { code:"PROT-009", name:"Adverse Reaction Escalation Pathway", categoryIds:["cat-safety"] },
  { code:"PROT-010", name:"Trial Eligibility Intake Form", categoryIds:["cat-screening"] }
];

/* Returns true if it added any required default categories. Protocols are
 * created only by the user or by cloud sync, never automatically. */
async function ensureSeeded(){
  let seeded = false;
  const existingCats = await dbGetAll(STORE_CATEGORIES);
  if(existingCats.length === 0){
    for(const c of SEED_CATEGORIES) await dbPut(STORE_CATEGORIES, c);
    seeded = true;
  }
  return seeded;
}

if(window.pdfjsLib){
  pdfjsLib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
}

/* ---------------- File <-> data URL (HHS-compatible storage) ----------------
 * Treatment options store their PDF as a base64 data URL in `fileData`, the
 * same shape the HHS app uses for trial.pdfFiles[].data. Data URLs are plain
 * strings, so the whole library JSON-serializes cleanly for export/sync —
 * unlike Blobs, which can't survive JSON.stringify.
 */
function fileToDataUrl(file){
  return new Promise((resolve,reject)=>{
    const reader = new FileReader();
    reader.onload = ()=> resolve(reader.result);
    reader.onerror = ()=> reject(reader.error);
    reader.readAsDataURL(file);
  });
}

function approxBytesFromDataUrl(dataUrl){
  if(!dataUrl) return 0;
  const base64 = dataUrl.slice(dataUrl.indexOf(",") + 1);
  return Math.floor(base64.length * 3 / 4);
}

function fmtBytes(bytes){
  if(bytes < 1024) return `${bytes} B`;
  if(bytes < 1024*1024) return `${(bytes/1024).toFixed(0)} KB`;
  return `${(bytes/(1024*1024)).toFixed(1)} MB`;
}

/* ---------------- PDF text extraction (for content search) ---------------- */
async function extractPdfText(file){
  if(!window.pdfjsLib) return { text:"", pageCount:null };
  try{
    const buf = await file.arrayBuffer();
    const pdf = await pdfjsLib.getDocument({ data: buf }).promise;
    const maxPages = Math.min(pdf.numPages, 40);
    let text = "";
    for(let i=1; i<=maxPages; i++){
      const page = await pdf.getPage(i);
      const content = await page.getTextContent();
      text += content.items.map(it => it.str).join(" ") + "\n";
      if(text.length > 200000) break;
    }
    return { text: text.trim(), pageCount: pdf.numPages };
  } catch(err){
    console.warn("PDF text extraction failed:", err);
    return { text:"", pageCount:null };
  }
}

function findSnippet(text, query, radius){
  radius = radius || 50;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if(idx === -1) return null;
  const start = Math.max(0, idx - radius);
  const end = Math.min(text.length, idx + query.length + radius);
  let snippet = text.slice(start, end).replace(/\s+/g, " ").trim();
  if(start > 0) snippet = "…" + snippet;
  if(end < text.length) snippet = snippet + "…";
  return snippet;
}

/* ---------------- Toasts (with optional Undo) ---------------- */
const toastStack = document.getElementById("toastStack");

function removeToastEl(el){
  if(!el || !el.parentNode) return;
  el.classList.add("toast-leaving");
  setTimeout(()=> el.remove(), 180);
}

function showToast(message, { type="", duration=2600 } = {}){
  const el = document.createElement("div");
  el.className = `toast ${type ? "toast-"+type : ""}`.trim();
  el.innerHTML = `<span class="toast-message">${escapeHTML(message)}</span>`;
  toastStack.appendChild(el);
  setTimeout(()=> removeToastEl(el), duration);
  return el;
}

/** Shows a toast with an Undo button for an action that has ALREADY been
 * persisted (caller's job, done before calling this). Undo re-applies
 * whatever reverses it — it does not cancel a pending write, because
 * deferring the write until the toast times out meant closing the tab or
 * refreshing before then silently discarded the delete. */
function showUndoToast(message, undo, { duration=5500 } = {}){
  const el = document.createElement("div");
  el.className = "toast";
  el.innerHTML = `<span class="toast-message">${escapeHTML(message)}</span><button class="toast-undo" type="button">Undo</button>`;
  toastStack.appendChild(el);

  const timeoutId = setTimeout(()=> removeToastEl(el), duration);

  el.querySelector(".toast-undo").addEventListener("click", async ()=>{
    clearTimeout(timeoutId);
    removeToastEl(el);
    try{ await undo(); } catch(err){ console.error(err); }
  });
}

/* ---------------- Recently viewed ---------------- */
const RECENT_KEY = "cph_recent_v1";
const RECENT_MAX = 6;
let recentOpen = false;

function getRecent(){
  try{ return JSON.parse(localStorage.getItem(RECENT_KEY)) || []; }
  catch{ return []; }
}
function saveRecent(list){
  try{ localStorage.setItem(RECENT_KEY, JSON.stringify(list)); } catch{ /* storage unavailable */ }
}
function addRecentlyViewed(protocol, treatment){
  let list = getRecent().filter(r => !(r.protocolId === protocol.id && r.treatmentId === treatment.id));
  list.unshift({ protocolId: protocol.id, treatmentId: treatment.id, protocolName: protocol.name, treatmentName: treatment.name });
  list = list.slice(0, RECENT_MAX);
  saveRecent(list);
  renderRecentChips();
}
function renderRecentChips(){
  const row = document.getElementById("recentRow");
  const chipsEl = document.getElementById("recentChips");
  const list = getRecent();
  if(list.length === 0){
    recentOpen = false;
    row.style.display = "none";
    document.getElementById("recentBtn").setAttribute("aria-expanded", "false");
    return;
  }
  row.style.display = recentOpen ? "flex" : "none";
  chipsEl.innerHTML = list.map((r,i) => `
    <div class="recent-entry">
      <button class="recent-chip" data-action="open" data-idx="${i}">
        <span class="recent-file-icon" aria-hidden="true">PDF</span>
        <span class="recent-file-copy">
          <strong>${escapeHTML(r.treatmentName)}</strong>
          <span>${escapeHTML(r.protocolName)}</span>
        </span>
        <span class="recent-open-icon" aria-hidden="true">›</span>
      </button>
      <button class="recent-remove" data-action="remove" data-idx="${i}" type="button" title="Remove from recently viewed" aria-label="Remove ${escapeHTML(r.treatmentName)} from recently viewed">×</button>
    </div>
  `).join("");
}
document.getElementById("recentChips").addEventListener("click", (e)=>{
  const btn = e.target.closest("[data-action]");
  if(!btn) return;
  const index = Number(btn.dataset.idx);
  const list = getRecent();
  const r = list[index];
  if(!r) return;
  if(btn.dataset.action === "remove"){
    list.splice(index, 1);
    saveRecent(list);
    renderRecentChips();
    return;
  }
  const protocol = allProtocols.find(p => p.id === r.protocolId);
  const treatment = protocol && protocol.treatmentOptions.find(t => t.id === r.treatmentId);
  if(protocol && treatment && treatment.fileData){
    openViewer(protocol, treatment);
  } else {
    showToast("That file is no longer available.", { type:"warning" });
  }
});

const libraryMenuBtn = document.getElementById("libraryMenuBtn");
const libraryMenu = document.getElementById("libraryMenu");
const recentBtn = document.getElementById("recentBtn");

function closeLibraryMenu(){
  libraryMenu.hidden = true;
  libraryMenuBtn.setAttribute("aria-expanded", "false");
}

libraryMenuBtn.addEventListener("click", (e)=>{
  e.stopPropagation();
  libraryMenu.hidden = !libraryMenu.hidden;
  libraryMenuBtn.setAttribute("aria-expanded", String(!libraryMenu.hidden));
});

recentBtn.addEventListener("click", ()=>{
  recentOpen = !recentOpen;
  recentBtn.setAttribute("aria-expanded", String(recentOpen));
  renderRecentChips();
  closeLibraryMenu();
});

document.addEventListener("click", (e)=>{
  if(!e.target.closest(".library-menu")) closeLibraryMenu();
});

/* ---------------- Sticky header shadow ---------------- */
const topHeader = document.querySelector("header.top");
window.addEventListener("scroll", ()=>{
  topHeader.classList.toggle("scrolled", window.scrollY > 4);
});

/* ---------------- Editable application titles ---------------- */
const BRAND_EYEBROW_KEY = "cph_brand_eyebrow";
const APP_TITLE_KEY = "cph_app_title";
const brandEyebrow = document.getElementById("brandEyebrow");
const appTitle = document.getElementById("appTitle");

function loadAppTitles(){
  try{
    const savedEyebrow = localStorage.getItem(BRAND_EYEBROW_KEY);
    const savedTitle = localStorage.getItem(APP_TITLE_KEY);
    if(savedEyebrow) brandEyebrow.textContent = savedEyebrow;
    if(savedTitle) appTitle.textContent = savedTitle;
  } catch{ /* storage unavailable */ }
  document.title = appTitle.textContent.trim() || "Clinical Protocol Hub";
}

function saveEditableTitle(element, key, fallback){
  const value = element.textContent.replace(/\s+/g, " ").trim() || fallback;
  element.textContent = value;
  try{ localStorage.setItem(key, value); } catch{ /* storage unavailable */ }
  if(element === appTitle) document.title = value;
  syncBrandingToCloud();
}

function applyCloudBranding(data){
  if(typeof data.eyebrow === "string" && data.eyebrow.trim()){
    brandEyebrow.textContent = data.eyebrow.trim();
    try{ localStorage.setItem(BRAND_EYEBROW_KEY, brandEyebrow.textContent); } catch{ /* storage unavailable */ }
  }
  if(typeof data.title === "string" && data.title.trim()){
    appTitle.textContent = data.title.trim();
    document.title = appTitle.textContent;
    try{ localStorage.setItem(APP_TITLE_KEY, appTitle.textContent); } catch{ /* storage unavailable */ }
  }
}

function syncBrandingToCloud(){
  if(!cloudDb || !cloudBrandingLoaded) return;
  cloudHub().collection(COL_SETTINGS).doc(BRANDING_DOC_ID).set({
    eyebrow: brandEyebrow.textContent.trim(),
    title: appTitle.textContent.trim(),
    updatedAt: new Date().toISOString()
  }, { merge:true }).catch(err => console.warn("Could not sync branding:", err));
}

brandEyebrow.addEventListener("blur", ()=> saveEditableTitle(brandEyebrow, BRAND_EYEBROW_KEY, "Nursing & Trials Station"));
appTitle.addEventListener("blur", ()=> saveEditableTitle(appTitle, APP_TITLE_KEY, "Clinical Protocol Hub"));
for(const element of [brandEyebrow, appTitle]){
  element.addEventListener("keydown", e => {
    if(e.key === "Enter"){
      e.preventDefault();
      element.blur();
    }
  });
}
loadAppTitles();

/* ---------------- State ---------------- */
let allProtocols = [];
let allCategories = [];
let currentQuery = "";
let activeCategoryId = null; // null = All
let sortMode = "name"; // "name" | "updated"

function categoryMap(){
  return new Map(allCategories.map(c => [c.id, c]));
}
function categoryName(id){
  const c = categoryMap().get(id);
  return c ? c.name : "Uncategorized";
}

/* ---------------- Helpers ---------------- */
function fmtDate(iso){
  const d = new Date(iso);
  return d.toLocaleDateString(undefined, { year:"numeric", month:"short", day:"numeric" });
}
function escapeHTML(s){
  return String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function highlight(text, query){
  if(!query) return escapeHTML(text);
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if(idx === -1) return escapeHTML(text);
  const before = escapeHTML(text.slice(0, idx));
  const match = escapeHTML(text.slice(idx, idx+query.length));
  const after = escapeHTML(text.slice(idx+query.length));
  return `${before}<mark>${match}</mark>${after}`;
}
function latestUpdate(protocol){
  let latest = protocol.updatedAt;
  for(const t of (protocol.treatmentOptions || [])){
    if(t.updatedAt > latest) latest = t.updatedAt;
  }
  return latest;
}
function protocolHasCategory(p, catId){
  if(p.categoryIds.includes(catId)) return true;
  return (p.treatmentOptions || []).some(t => (t.categoryIds || []).includes(catId));
}
function protocolMatchesQuery(p, q){
  if(!q) return true;
  if(p.name.toLowerCase().includes(q)) return true;
  if(p.categoryIds.some(id => categoryName(id).toLowerCase().includes(q))) return true;
  return (p.treatmentOptions || []).some(t =>
    t.name.toLowerCase().includes(q) ||
    (t.fileName && t.fileName.toLowerCase().includes(q)) ||
    (t.fileText && t.fileText.toLowerCase().includes(q)) ||
    (t.categoryIds || []).some(id => categoryName(id).toLowerCase().includes(q))
  );
}

function nextProtocolCode(){
  let max = 0;
  for(const p of allProtocols){
    const m = /^PROT-(\d+)$/.exec(p.code || "");
    if(m) max = Math.max(max, parseInt(m[1], 10));
  }
  return `PROT-${String(max + 1).padStart(3,"0")}`;
}

/* ---------------- Migration for records from older versions ----------------
 * Note: this is sync-only. Blob -> data URL conversion is async, so it's
 * handled separately in migrateBlobsToDataUrls() during load.
 */
function normalizeProtocol(p){
  let changed = false;

  if(!Array.isArray(p.treatmentOptions)){
    p.treatmentOptions = [];
    // Older single-PDF version stored fileBlob/fileName directly on the protocol —
    // carry that file over as its first treatment option instead of losing it.
    if(p.fileBlob){
      p.treatmentOptions.push({
        id: crypto.randomUUID(),
        name: p.fileName || "Uploaded document",
        fileBlob: p.fileBlob,
        fileName: p.fileName || null,
        updatedAt: p.updatedAt || new Date().toISOString()
      });
    }
    delete p.fileBlob;
    delete p.fileName;
    changed = true;
  }

  for(const t of p.treatmentOptions){
    if(t.fileText === undefined){ t.fileText = ""; changed = true; }
    if(t.pageCount === undefined){ t.pageCount = null; changed = true; }
    if(!Array.isArray(t.categoryIds)){ t.categoryIds = []; changed = true; }
  }

  if(!Array.isArray(p.categoryIds)){
    let catId = UNCATEGORIZED_ID;
    if(p.categoryId){
      catId = p.categoryId;
    } else if(p.category){
      const match = allCategories.find(c => c.name.toLowerCase() === String(p.category).toLowerCase());
      if(match) catId = match.id;
    }
    p.categoryIds = [catId];
    delete p.categoryId;
    delete p.category;
    changed = true;
  } else if(p.categoryIds.length === 0){
    p.categoryIds = [UNCATEGORIZED_ID];
    changed = true;
  }

  if(!p.updatedAt){
    p.updatedAt = new Date().toISOString();
    changed = true;
  }

  if(typeof p.pinned !== "boolean"){
    p.pinned = false;
    changed = true;
  }

  return changed;
}

/* ---------------- Blob -> data URL migration (async) ---------------- */
async function migrateBlobsToDataUrls(){
  let migrated = 0;
  for(const p of allProtocols){
    let changed = false;
    for(const t of p.treatmentOptions){
      if(!t.fileData && t.fileBlob){
        try{
          t.fileData = await fileToDataUrl(t.fileBlob);
          delete t.fileBlob;
          changed = true;
          migrated++;
        } catch(err){
          console.warn("Could not convert stored file to data URL:", err);
        }
      }
    }
    if(changed) await dbPut(STORE_PROTOCOLS, p);
  }
  return migrated;
}

/* ---------------- Load ---------------- */
async function loadAll(){
  [allProtocols, allCategories] = await Promise.all([
    dbGetAll(STORE_PROTOCOLS),
    dbGetAll(STORE_CATEGORIES)
  ]);

  for(const p of allProtocols){
    if(normalizeProtocol(p)){
      await dbPut(STORE_PROTOCOLS, p);
    }
  }

  await migrateBlobsToDataUrls();

  renderChips();
  renderGrid();
  renderRecentChips();
  renderStorageSummary();
}

/* ---------------- Category chips ---------------- */
function renderChips(){
  const wrap = document.getElementById("categoryChips");
  const counts = new Map();
  for(const c of allCategories){
    counts.set(c.id, allProtocols.filter(p => protocolHasCategory(p, c.id)).length);
  }
  const sorted = [...allCategories].sort((a,b)=> a.name.localeCompare(b.name));
  const chips = [
    `<button class="chip ${activeCategoryId===null?'active':''}" data-cat="__all__">All (${allProtocols.length})</button>`,
    ...sorted.map(c => `<button class="chip ${activeCategoryId===c.id?'active':''}" data-cat="${c.id}">${escapeHTML(c.name)} (${counts.get(c.id)||0})</button>`)
  ];
  wrap.innerHTML = chips.join("");
}
document.getElementById("categoryChips").addEventListener("click", (e)=>{
  const btn = e.target.closest(".chip");
  if(!btn) return;
  activeCategoryId = btn.dataset.cat === "__all__" ? null : btn.dataset.cat;
  renderChips();
  renderGrid();
});

/* ---------------- Grid rendering ---------------- */
function renderGrid(){
  const grid = document.getElementById("grid");
  const empty = document.getElementById("emptyState");
  const q = currentQuery.trim().toLowerCase();

  const filtered = allProtocols
    .filter(p => activeCategoryId === null || protocolHasCategory(p, activeCategoryId))
    .filter(p => protocolMatchesQuery(p, q))
    .sort((a,b)=>{
      if(a.pinned !== b.pinned) return a.pinned ? -1 : 1;
      return sortMode === "updated"
        ? latestUpdate(b).localeCompare(latestUpdate(a))
        : a.name.localeCompare(b.name);
    });

  document.getElementById("resultCount").textContent =
    q ? `${filtered.length} match${filtered.length===1?"":"es"} for "${currentQuery.trim()}"` : `${filtered.length} protocol${filtered.length===1?"":"s"} shown`;

  if(filtered.length === 0){
    grid.style.display = "none";
    empty.style.display = "block";
    document.getElementById("emptyTitle").textContent = q ? `No protocol matched "${currentQuery.trim()}"` : "No protocols in this category yet";
    return;
  }
  grid.style.display = "grid";
  empty.style.display = "none";

  grid.innerHTML = filtered.map(p => `
    <div class="card ${p.pinned ? "pinned" : ""}" data-id="${p.id}">
      <div class="card-code mono">${escapeHTML(p.code)}</div>
      <div class="card-header">
        <h3 class="card-title">${highlight(p.name, currentQuery)}</h3>
        <div class="card-icons">
          <button class="pin-btn ${p.pinned ? "active" : ""}" data-action="toggle-pin" data-id="${p.id}" title="${p.pinned ? "Unpin" : "Pin to top"}">${p.pinned ? "★" : "☆"}</button>
          <button class="delete-icon-btn" data-action="duplicate-protocol" data-id="${p.id}" title="Duplicate protocol">⧉</button>
          <button class="delete-icon-btn" data-action="edit-protocol" data-id="${p.id}" title="Edit protocol">✎</button>
          <button class="delete-icon-btn" data-action="delete-protocol" data-id="${p.id}" title="Delete protocol">✕</button>
        </div>
      </div>
      <div class="card-meta">
        ${p.categoryIds.map(id => `<span class="tag">${escapeHTML(categoryName(id))}</span>`).join("")}
        <span>Updated ${fmtDate(latestUpdate(p))}</span>
        ${p.pinned ? `<span class="tag pinned-tag">📌 Pinned</span>` : ""}
      </div>
      <div class="treatment-list">
        ${p.treatmentOptions.length === 0
          ? `<div class="no-treatments">No treatment options yet</div>`
          : p.treatmentOptions.map(t => {
              const nameMatches = q && t.name.toLowerCase().includes(q);
              let snippetHtml = "";
              if(q && !nameMatches && t.fileText && t.fileText.toLowerCase().includes(q)){
                const snippet = findSnippet(t.fileText, currentQuery.trim());
                if(snippet) snippetHtml = `<div class="treatment-snippet">📄 ${highlight(snippet, currentQuery)}</div>`;
              }
              return `
            <div class="treatment-item">
              <div class="treatment-item-row">
                <span class="treatment-item-name">${highlight(t.name, currentQuery)}</span>
                ${t.pageCount ? `<span class="page-count-tag">${t.pageCount}p</span>` : ""}
                ${(t.categoryIds || []).map(id => `<span class="mini-tag">${escapeHTML(categoryName(id))}</span>`).join("")}
                <div class="treatment-actions">
                  ${t.fileData
                    ? `<button class="mini-btn view" data-action="view-treatment" data-id="${p.id}" data-tid="${t.id}">View</button>
                       <button class="mini-btn icon" data-action="edit-treatment" data-id="${p.id}" data-tid="${t.id}" title="Rename or replace file">✎</button>`
                    : (t.fileName && (t.fileUrl || t.filePartCount)
                        ? `<button class="mini-btn view" disabled title="Downloading from the cloud">Syncing…</button>
                           <button class="mini-btn icon" data-action="edit-treatment" data-id="${p.id}" data-tid="${t.id}" title="Rename or replace file">✎</button>`
                        : `<button class="mini-btn view" data-action="edit-treatment" data-id="${p.id}" data-tid="${t.id}">Upload</button>`)
                  }
                  <button class="mini-btn icon" data-action="delete-treatment" data-id="${p.id}" data-tid="${t.id}" title="Delete">🗑</button>
                </div>
              </div>
              ${snippetHtml}
            </div>
          `;
            }).join("")
        }
      </div>
      <button class="add-treatment-btn" data-action="add-treatment" data-id="${p.id}">+ Add treatment option</button>
    </div>
  `).join("");
}

/* ---------------- Search ---------------- */
const searchInput = document.getElementById("search");
const searchClearBtn = document.getElementById("searchClear");

function updateSearchClearVisibility(){
  searchClearBtn.classList.toggle("show", searchInput.value.length > 0);
}
searchInput.addEventListener("input", (e)=>{
  currentQuery = e.target.value;
  updateSearchClearVisibility();
  renderGrid();
});
searchClearBtn.addEventListener("click", ()=>{
  searchInput.value = "";
  currentQuery = "";
  updateSearchClearVisibility();
  renderGrid();
  searchInput.focus();
});

/* ---------------- Sorting ---------------- */
document.getElementById("sortSelect").addEventListener("change", (e)=>{
  sortMode = e.target.value;
  renderGrid();
});

/* ---------------- Keyboard shortcuts ---------------- */
function anyOverlayOpen(){
  return [viewerOverlay, formOverlay, treatmentOverlay, categoryOverlay]
    .some(el => el && el.classList.contains("show"));
}
function closeAnyOpenOverlay(){
  if(viewerOverlay.classList.contains("show")) return closeViewer();
  if(formOverlay.classList.contains("show")) return closeProtocolForm();
  if(treatmentOverlay.classList.contains("show")) return closeTreatmentForm();
  if(categoryOverlay.classList.contains("show")) return categoryOverlay.classList.remove("show");
}
document.addEventListener("keydown", (e)=>{
  if(e.key === "Escape" && anyOverlayOpen()){
    closeAnyOpenOverlay();
    return;
  }
  if(e.key === "/" && !anyOverlayOpen()){
    const tag = document.activeElement && document.activeElement.tagName;
    if(tag !== "INPUT" && tag !== "TEXTAREA" && tag !== "SELECT"){
      e.preventDefault();
      searchInput.focus();
    }
  }
});

/* ---------------- PDF Viewer ---------------- */
const viewerOverlay = document.getElementById("viewerOverlay");
let viewerObjectUrl = null;

function dataUrlToBlob(dataUrl){
  const [header, encoded] = dataUrl.split(",");
  const mime = /data:([^;]+)/.exec(header)?.[1] || "application/pdf";
  const binary = atob(encoded);
  const bytes = new Uint8Array(binary.length);
  for(let i=0; i<binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return new Blob([bytes], { type:mime });
}

function openViewer(protocol, treatment){
  const url = treatment.fileData;
  if(viewerObjectUrl) URL.revokeObjectURL(viewerObjectUrl);
  viewerObjectUrl = isDataUrl(url) ? URL.createObjectURL(dataUrlToBlob(url)) : url;
  document.getElementById("viewerTitle").textContent = `${protocol.name} — ${treatment.name}`;
  document.getElementById("viewerCode").textContent = `${protocol.code} · ${protocol.categoryIds.map(categoryName).join(", ")}`;
  document.getElementById("viewerFrame").src = viewerObjectUrl;
  document.getElementById("viewerOpenNewTab").href = viewerObjectUrl;
  viewerOverlay.classList.add("show");
  addRecentlyViewed(protocol, treatment);
}
function closeViewer(){
  viewerOverlay.classList.remove("show");
  document.getElementById("viewerFrame").src = "";
  if(viewerObjectUrl){
    URL.revokeObjectURL(viewerObjectUrl);
    viewerObjectUrl = null;
  }
}
document.getElementById("viewerClose").addEventListener("click", closeViewer);
viewerOverlay.addEventListener("click", (e)=>{ if(e.target === viewerOverlay) closeViewer(); });

/* ---------------- Category checkbox lists (shared by both forms) ---------------- */
function renderCatCheckboxList(containerEl, selectedIds){
  const sorted = [...allCategories].sort((a,b)=> a.name.localeCompare(b.name));
  containerEl.innerHTML = sorted.map(c => `
    <label class="cat-checkbox">
      <input type="checkbox" value="${c.id}" ${selectedIds.includes(c.id) ? "checked" : ""}>
      <span>${escapeHTML(c.name)}</span>
    </label>
  `).join("");
}
function getCheckedCategoryIds(containerEl){
  return [...containerEl.querySelectorAll("input[type=checkbox]:checked")].map(cb => cb.value);
}
/** Wires an inline "+ New category" input+button next to a checkbox list:
 * creates (or reuses) the category, checks it, and re-renders the list
 * without losing whatever else was already checked. */
function wireCategoryAdder(listEl, inputEl, btnEl){
  const create = async ()=>{
    const name = inputEl.value.trim();
    if(!name) return;
    let cat = allCategories.find(c => c.name.toLowerCase() === name.toLowerCase());
    if(!cat){
      cat = { id: crypto.randomUUID(), name };
      await dbPut(STORE_CATEGORIES, cat);
      allCategories.push(cat);
    }
    const selected = getCheckedCategoryIds(listEl);
    if(!selected.includes(cat.id)) selected.push(cat.id);
    renderCatCheckboxList(listEl, selected);
    inputEl.value = "";
    renderChips();
    showToast(`Added category "${cat.name}"`, { type:"success" });
  };
  btnEl.addEventListener("click", create);
  inputEl.addEventListener("keydown", (e)=>{
    if(e.key === "Enter"){ e.preventDefault(); create(); }
  });
}

/* ---------------- Add / Edit protocol form ---------------- */
const formOverlay = document.getElementById("formOverlay");
const protocolForm = document.getElementById("protocolForm");
const pCategoryList = document.getElementById("pCategoryList");
let protocolFormMode = "add"; // "add" | "edit"
let protocolFormTargetId = null;

wireCategoryAdder(pCategoryList, document.getElementById("pNewCategoryName"), document.getElementById("pAddCategoryBtn"));

function openProtocolForm(mode, protocol){
  protocolFormMode = mode;
  protocolFormTargetId = protocol ? protocol.id : null;
  document.getElementById("pNameWarning").style.display = "none";
  if(mode === "add"){
    document.getElementById("formTitle").textContent = "Add protocol";
    document.getElementById("formSub").textContent = "Name it, tag it with one or more categories, and attach treatment options after.";
    document.getElementById("pName").value = "";
    renderCatCheckboxList(pCategoryList, [UNCATEGORIZED_ID]);
  } else {
    document.getElementById("formTitle").textContent = "Edit protocol";
    document.getElementById("formSub").textContent = protocol.code;
    document.getElementById("pName").value = protocol.name;
    renderCatCheckboxList(pCategoryList, protocol.categoryIds.length ? protocol.categoryIds : [UNCATEGORIZED_ID]);
  }
  formOverlay.classList.add("show");
  setTimeout(()=> document.getElementById("pName").focus(), 0);
}
function closeProtocolForm(){ formOverlay.classList.remove("show"); }
document.getElementById("addProtocolBtn").addEventListener("click", ()=> openProtocolForm("add", null));
document.getElementById("formCancel").addEventListener("click", closeProtocolForm);
formOverlay.addEventListener("click", (e)=>{ if(e.target === formOverlay) closeProtocolForm(); });

const pNameInput = document.getElementById("pName");
const pNameWarning = document.getElementById("pNameWarning");
pNameInput.addEventListener("input", ()=>{
  const name = pNameInput.value.trim().toLowerCase();
  const dup = name && allProtocols.some(p => p.name.toLowerCase() === name && p.id !== protocolFormTargetId);
  pNameWarning.style.display = dup ? "block" : "none";
});

protocolForm.addEventListener("submit", async (e)=>{
  e.preventDefault();
  const name = document.getElementById("pName").value.trim();
  if(!name){ showToast("Give the protocol a name.", { type:"warning" }); pNameInput.focus(); return; }

  let categoryIds = getCheckedCategoryIds(pCategoryList);
  if(categoryIds.length === 0) categoryIds = [UNCATEGORIZED_ID];

  if(protocolFormMode === "add"){
    await dbPut(STORE_PROTOCOLS, {
      id: crypto.randomUUID(),
      code: nextProtocolCode(),
      name, categoryIds,
      updatedAt: new Date().toISOString(),
      pinned: false,
      treatmentOptions: []
    });
    closeProtocolForm();
    await loadAll();
    showToast(`Added "${name}"`, { type:"success" });
  } else {
    const protocol = allProtocols.find(p => p.id === protocolFormTargetId);
    protocol.name = name;
    protocol.categoryIds = categoryIds;
    protocol.updatedAt = new Date().toISOString();
    await dbPut(STORE_PROTOCOLS, protocol);
    closeProtocolForm();
    await loadAll();
    showToast(`Saved "${name}"`, { type:"success" });
  }
});

async function deleteProtocol(id){
  const idx = allProtocols.findIndex(p => p.id === id);
  if(idx === -1) return;
  const [protocol] = allProtocols.splice(idx, 1);
  renderChips();
  renderGrid();
  await dbDelete(STORE_PROTOCOLS, id);
  showUndoToast(`Deleted "${protocol.name}"`, async ()=>{
    allProtocols.push(protocol);
    await dbPut(STORE_PROTOCOLS, protocol);
    renderChips();
    renderGrid();
  });
}

/* ---------------- Add / Edit treatment option form ---------------- */
const treatmentOverlay = document.getElementById("treatmentOverlay");
const treatmentForm = document.getElementById("treatmentForm");
let treatmentFormMode = "add"; // "add" | "edit"
let treatmentFormProtocolId = null;
let treatmentFormTreatmentId = null;

function openTreatmentForm(mode, protocolId, treatmentId){
  treatmentFormMode = mode;
  treatmentFormProtocolId = protocolId;
  treatmentFormTreatmentId = treatmentId || null;
  document.getElementById("tFile").value = "";
  document.getElementById("tNameWarning").style.display = "none";

  if(mode === "add"){
    document.getElementById("treatmentFormTitle").textContent = "Add treatment option";
    document.getElementById("treatmentFormSub").textContent = "Name this treatment option and attach its PDF.";
    document.getElementById("tName").value = "";
    document.getElementById("tFileHint").textContent = "You can also add it now and upload the PDF later.";
    renderCatCheckboxList(tCategoryList, []);
  } else {
    const protocol = allProtocols.find(p => p.id === protocolId);
    const treatment = protocol.treatmentOptions.find(t => t.id === treatmentId);
    document.getElementById("treatmentFormTitle").textContent = "Edit treatment option";
    document.getElementById("treatmentFormSub").textContent = protocol.name;
    document.getElementById("tName").value = treatment.name;
    let hint = treatment.fileName ? `Current file: ${treatment.fileName}. Leave blank to keep it.` : "No PDF attached yet.";
    if(treatment.fileName){
      hint += treatment.fileText ? " Text is indexed for search." : " No searchable text found (likely a scanned document).";
    }
    document.getElementById("tFileHint").textContent = hint;
    renderCatCheckboxList(tCategoryList, treatment.categoryIds || []);
  }
  treatmentOverlay.classList.add("show");
  setTimeout(()=> document.getElementById("tName").focus(), 0);
}
function closeTreatmentForm(){ treatmentOverlay.classList.remove("show"); }
document.getElementById("treatmentCancel").addEventListener("click", closeTreatmentForm);
treatmentOverlay.addEventListener("click", (e)=>{ if(e.target === treatmentOverlay) closeTreatmentForm(); });

const treatmentSubmitBtn = document.getElementById("treatmentSubmit");
const tDropzone = document.getElementById("tDropzone");
const tFileInput = document.getElementById("tFile");
const tNameInput = document.getElementById("tName");
const tNameWarning = document.getElementById("tNameWarning");
const tCategoryList = document.getElementById("tCategoryList");

wireCategoryAdder(tCategoryList, document.getElementById("tNewCategoryName"), document.getElementById("tAddCategoryBtn"));

tNameInput.addEventListener("input", ()=>{
  const protocol = allProtocols.find(p => p.id === treatmentFormProtocolId);
  const name = tNameInput.value.trim().toLowerCase();
  const dup = protocol && name && protocol.treatmentOptions.some(t => t.name.toLowerCase() === name && t.id !== treatmentFormTreatmentId);
  tNameWarning.style.display = dup ? "block" : "none";
});

tDropzone.addEventListener("dragover", (e)=>{ e.preventDefault(); tDropzone.classList.add("drag-over"); });
tDropzone.addEventListener("dragleave", ()=> tDropzone.classList.remove("drag-over"));
tDropzone.addEventListener("drop", (e)=>{
  e.preventDefault();
  tDropzone.classList.remove("drag-over");
  const file = e.dataTransfer.files[0];
  if(!file) return;
  if(file.type !== "application/pdf" && !file.name.toLowerCase().endsWith(".pdf")){
    showToast("Please drop a PDF file.", { type:"warning" });
    return;
  }
  const dt = new DataTransfer();
  dt.items.add(file);
  tFileInput.files = dt.files;
});

treatmentForm.addEventListener("submit", async (e)=>{
  e.preventDefault();
  const name = tNameInput.value.trim();
  if(!name){ showToast("Give the treatment option a name.", { type:"warning" }); tNameInput.focus(); return; }
  const file = tFileInput.files[0] || null;
  const protocol = allProtocols.find(p => p.id === treatmentFormProtocolId);
  const now = new Date().toISOString();

  let fileText, pageCount, fileData;
  if(file){
    treatmentSubmitBtn.disabled = true;
    treatmentSubmitBtn.textContent = "Reading PDF…";
    const extracted = await extractPdfText(file);
    fileText = extracted.text;
    pageCount = extracted.pageCount;
    fileData = await fileToDataUrl(file);
    treatmentSubmitBtn.disabled = false;
    treatmentSubmitBtn.textContent = "Save";
  }

  if(treatmentFormMode === "add"){
    protocol.treatmentOptions.push({
      id: crypto.randomUUID(),
      name,
      fileData: file ? fileData : null,
      fileName: file ? file.name : null,
      fileText: file ? fileText : "",
      pageCount: file ? pageCount : null,
      categoryIds: getCheckedCategoryIds(tCategoryList),
      updatedAt: now
    });
  } else {
    const treatment = protocol.treatmentOptions.find(t => t.id === treatmentFormTreatmentId);
    treatment.name = name;
    if(file){
      treatment.fileData = fileData;
      treatment.fileName = file.name;
      treatment.fileText = fileText;
      treatment.pageCount = pageCount;
    }
    treatment.categoryIds = getCheckedCategoryIds(tCategoryList);
    treatment.updatedAt = now;
  }
  protocol.updatedAt = now;
  await dbPut(STORE_PROTOCOLS, protocol);
  closeTreatmentForm();
  await loadAll();
  showToast(`Saved "${name}"`, { type:"success" });
});

async function deleteTreatment(protocolId, treatmentId){
  const protocol = allProtocols.find(p => p.id === protocolId);
  const idx = protocol.treatmentOptions.findIndex(t => t.id === treatmentId);
  if(idx === -1) return;
  const [treatment] = protocol.treatmentOptions.splice(idx, 1);
  protocol.updatedAt = new Date().toISOString();
  renderGrid();
  await dbPut(STORE_PROTOCOLS, protocol);
  showUndoToast(`Deleted "${treatment.name}"`, async ()=>{
    protocol.treatmentOptions.splice(idx, 0, treatment);
    protocol.updatedAt = new Date().toISOString();
    await dbPut(STORE_PROTOCOLS, protocol);
    renderGrid();
  });
}

/* ---------------- Card action delegation ---------------- */
document.getElementById("grid").addEventListener("click", (e)=>{
  const btn = e.target.closest("button[data-action]");
  if(!btn) return;
  const action = btn.dataset.action;
  const id = btn.dataset.id;
  const tid = btn.dataset.tid;
  const protocol = allProtocols.find(p => p.id === id);
  if(!protocol) return;

  if(action === "edit-protocol") openProtocolForm("edit", protocol);
  else if(action === "delete-protocol") deleteProtocol(id);
  else if(action === "duplicate-protocol") duplicateProtocol(id);
  else if(action === "toggle-pin") togglePin(id);
  else if(action === "add-treatment") openTreatmentForm("add", id, null);
  else if(action === "edit-treatment") openTreatmentForm("edit", id, tid);
  else if(action === "delete-treatment") deleteTreatment(id, tid);
  else if(action === "view-treatment"){
    const treatment = protocol.treatmentOptions.find(t => t.id === tid);
    if(treatment && treatment.fileData) openViewer(protocol, treatment);
  }
});

async function togglePin(id){
  const protocol = allProtocols.find(p => p.id === id);
  if(!protocol) return;
  protocol.pinned = !protocol.pinned;
  await dbPutLocalOnly(STORE_PROTOCOLS, protocol);
  renderChips();
  renderGrid();
}

async function duplicateProtocol(id){
  const original = allProtocols.find(p => p.id === id);
  if(!original) return;
  const now = new Date().toISOString();
  const copy = {
    id: crypto.randomUUID(),
    code: nextProtocolCode(),
    name: `${original.name} (Copy)`,
    categoryIds: [...original.categoryIds],
    pinned: false,
    updatedAt: now,
    treatmentOptions: original.treatmentOptions.map(t => ({
      id: crypto.randomUUID(),
      name: t.name,
      fileData: t.fileData || null,
      fileUrl: t.fileUrl || null,
      fileName: t.fileName || null,
      fileText: t.fileText || "",
      pageCount: t.pageCount || null,
      categoryIds: [...(t.categoryIds || [])],
      updatedAt: now
    }))
  };
  allProtocols.push(copy);
  await dbPut(STORE_PROTOCOLS, copy);
  renderChips();
  renderGrid();
  showToast(`Duplicated as "${copy.name}"`, { type:"success" });
}

/* ---------------- Manage categories ---------------- */
const categoryOverlay = document.getElementById("categoryOverlay");
document.getElementById("manageCatsBtn").addEventListener("click", ()=>{
  renderCategoryManager();
  categoryOverlay.classList.add("show");
});
document.getElementById("categoryOverlayClose").addEventListener("click", ()=>{
  categoryOverlay.classList.remove("show");
});
categoryOverlay.addEventListener("click", (e)=>{ if(e.target === categoryOverlay) categoryOverlay.classList.remove("show"); });

function renderCategoryManager(){
  const list = document.getElementById("catList");
  const counts = new Map();
  for(const c of allCategories){
    counts.set(c.id, allProtocols.filter(p => protocolHasCategory(p, c.id)).length);
  }
  const sorted = [...allCategories].sort((a,b)=> a.name.localeCompare(b.name));
  list.innerHTML = sorted.map(c => `
    <div class="cat-item" data-cat-id="${c.id}">
      <input type="text" value="${escapeHTML(c.name)}" data-role="cat-name" ${c.id===UNCATEGORIZED_ID ? "disabled" : ""}>
      <span class="count">${counts.get(c.id)||0} protocol${(counts.get(c.id)||0)===1?"":"s"}</span>
      ${c.id===UNCATEGORIZED_ID ? "" : `<button class="delete-icon-btn" data-role="cat-delete" title="Delete category">✕</button>`}
    </div>
  `).join("");
}

document.getElementById("catList").addEventListener("change", async (e)=>{
  const input = e.target.closest("input[data-role='cat-name']");
  if(!input) return;
  const row = input.closest(".cat-item");
  const catId = row.dataset.catId;
  const newName = input.value.trim();
  if(!newName) return;
  const cat = allCategories.find(c => c.id === catId);
  cat.name = newName;
  await dbPut(STORE_CATEGORIES, cat);
  await loadAll();
  renderCategoryManager();
  showToast(`Category renamed to "${newName}"`, { type:"success" });
});

document.getElementById("catList").addEventListener("click", async (e)=>{
  const btn = e.target.closest("button[data-role='cat-delete']");
  if(!btn) return;
  const row = btn.closest(".cat-item");
  const catId = row.dataset.catId;
  const catIdx = allCategories.findIndex(c => c.id === catId);
  if(catIdx === -1) return;
  const [cat] = allCategories.splice(catIdx, 1);

  const affected = allProtocols.filter(p => protocolHasCategory(p, catId));
  const snapshots = affected.map(p => ({
    protocol: p,
    prevCategoryIds: [...p.categoryIds],
    prevTreatmentCategoryIds: p.treatmentOptions.map(t => ({ id: t.id, ids: [...(t.categoryIds || [])] }))
  }));

  for(const p of affected){
    p.categoryIds = p.categoryIds.filter(id => id !== catId);
    if(p.categoryIds.length === 0) p.categoryIds = [UNCATEGORIZED_ID];
    for(const t of p.treatmentOptions){
      if(t.categoryIds) t.categoryIds = t.categoryIds.filter(id => id !== catId);
    }
  }
  if(activeCategoryId === catId) activeCategoryId = null;

  renderChips();
  renderGrid();
  renderCategoryManager();

  await dbDelete(STORE_CATEGORIES, catId);
  for(const p of affected) await dbPut(STORE_PROTOCOLS, p);

  const message = affected.length
    ? `Deleted "${cat.name}" — ${affected.length} protocol${affected.length===1?"":"s"} untagged`
    : `Deleted "${cat.name}"`;

  showUndoToast(message, async ()=>{
    allCategories.splice(catIdx, 0, cat);
    await dbPut(STORE_CATEGORIES, cat);
    for(const snap of snapshots){
      snap.protocol.categoryIds = snap.prevCategoryIds;
      for(const t of snap.protocol.treatmentOptions){
        const rec = snap.prevTreatmentCategoryIds.find(r => r.id === t.id);
        if(rec) t.categoryIds = rec.ids;
      }
      await dbPut(STORE_PROTOCOLS, snap.protocol);
    }
    renderChips(); renderGrid(); renderCategoryManager();
  });
});

document.getElementById("catAddBtn").addEventListener("click", async ()=>{
  const input = document.getElementById("catAddInput");
  const name = input.value.trim();
  if(!name) return;
  const dup = allCategories.find(c => c.name.toLowerCase() === name.toLowerCase());
  if(dup){ showToast("That category already exists.", { type:"warning" }); return; }
  await dbPut(STORE_CATEGORIES, { id: crypto.randomUUID(), name });
  input.value = "";
  await loadAll();
  renderCategoryManager();
  showToast(`Added category "${name}"`, { type:"success" });
});

/* ---------------- Export / Import (HHS-style portable library) ----------------
 * Because PDFs live as base64 data URL strings, the entire library —
 * protocols, categories, and the PDFs themselves — serializes to one JSON
 * file. This mirrors exportDatabaseToFile()/handleTxtImport() in the HHS app.
 */
function buildExportPayload(){
  return {
    format: "clinical-protocol-hub",
    version: 1,
    exportedAt: new Date().toISOString(),
    categories: allCategories,
    protocols: allProtocols
  };
}

document.getElementById("exportBtn").addEventListener("click", ()=>{
  try{
    const json = JSON.stringify(buildExportPayload());
    const blob = new Blob([json], { type:"application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `protocol-hub-${new Date().toISOString().slice(0,10)}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=> URL.revokeObjectURL(url), 1000);
    showToast(`Exported ${allProtocols.length} protocols (${fmtBytes(json.length)})`, { type:"success" });
  } catch(err){
    console.error("Export failed:", err);
    showToast("Export failed — the library may be too large for one file.", { type:"warning" });
  }
});

document.getElementById("importBtn").addEventListener("click", ()=>{
  document.getElementById("importInput").click();
});

document.getElementById("importInput").addEventListener("change", (e)=>{
  const file = e.target.files[0];
  if(!file) return;
  const reader = new FileReader();
  reader.onload = async (ev)=>{
    try{
      const payload = JSON.parse(ev.target.result);
      if(!payload || !Array.isArray(payload.protocols)){
        showToast("That file doesn't look like a Protocol Hub export.", { type:"warning" });
        return;
      }
      await applyImport(payload);
    } catch(err){
      console.error("Import failed:", err);
      showToast("Couldn't read that file: " + err.message, { type:"warning" });
    }
    e.target.value = "";
  };
  reader.readAsText(file);
});

/** Merges an imported library into the current one. Existing records with the
 * same id are overwritten; everything else is added, so importing never
 * silently discards protocols already on this device. */
async function applyImport(payload){
  let catCount = 0, protoCount = 0;

  if(Array.isArray(payload.categories)){
    for(const c of payload.categories){
      if(!c || !c.id || !c.name) continue;
      await dbPut(STORE_CATEGORIES, { id: c.id, name: c.name });
      catCount++;
    }
  }

  for(const p of payload.protocols){
    if(!p || !p.id || !p.name) continue;
    normalizeProtocol(p);
    await dbPut(STORE_PROTOCOLS, p);
    protoCount++;
  }

  await loadAll();
  showToast(`Imported ${protoCount} protocols and ${catCount} categories`, { type:"success" });
}

/* ---------------- Storage summary ---------------- */
async function renderStorageSummary(){
  let pdfCount = 0, pdfBytes = 0;
  for(const p of allProtocols){
    for(const t of p.treatmentOptions){
      if(t.fileData){ pdfCount++; if(isDataUrl(t.fileData)) pdfBytes += approxBytesFromDataUrl(t.fileData); }
    }
  }

  let quotaText = "";
  if(navigator.storage && navigator.storage.estimate){
    try{
      const { usage, quota } = await navigator.storage.estimate();
      if(quota) quotaText = ` · ${fmtBytes(usage)} of ~${fmtBytes(quota)} browser storage used`;
    } catch{ /* estimate unsupported */ }
  }

  document.getElementById("storageSummary").textContent =
    `${allProtocols.length} protocols · ${pdfCount} PDF${pdfCount===1?"":"s"} stored (${fmtBytes(pdfBytes)})${quotaText}${cloudStatusText()}`;
}

/* ---------------- Init ---------------- */
(async function init(){
  try{
    await loadAll();                                   // show this device's data immediately
    const cloudSync = initializeCloudSync();           // never rejects; runs in the background
    // A brand-new device has nothing to show, so give the cloud a moment to
    // deliver the shared library before falling back to starter data.
    if(allProtocols.length === 0 || allCategories.length === 0) await cloudSync;
    if(await ensureSeeded()) await loadAll();
  } catch(err){
    console.error("Startup failed:", err);
    const empty = document.getElementById("emptyState");
    const grid = document.getElementById("grid");
    if(grid) grid.style.display = "none";
    if(empty){
      empty.style.display = "block";
      document.getElementById("emptyTitle").textContent = "Couldn't open local storage";
      document.getElementById("emptySub").textContent =
        err.message + " Try closing other tabs running this app and reloading.";
    }
  }
})();