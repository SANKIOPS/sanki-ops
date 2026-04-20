 'use strict';
const express = require('express');
const Database = require('better-sqlite3');
const bcrypt = require('bcryptjs');  
const session = require('express-session');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
 
// ═══ DATABASE ═══════════════════════════════════════════════
const db = new Database(process.env.DB_PATH || './sanki_ops.db');
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'staff',
    department TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
  );

  CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_number TEXT UNIQUE NOT NULL,
    vendor_name TEXT NOT NULL,
    vendor_contact TEXT DEFAULT '',
    vendor_email TEXT DEFAULT '',
    order_date TEXT NOT NULL,
    expected_arrival TEXT DEFAULT '',
    actual_arrival TEXT DEFAULT '',
    status TEXT DEFAULT 'sent',
    total_amount REAL DEFAULT 0,
    currency TEXT DEFAULT 'CNY',
    inr_rate REAL DEFAULT 11.5,
    tracking_number TEXT DEFAULT '',
    courier TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS po_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    po_id INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    sku TEXT DEFAULT '',
    color TEXT DEFAULT '',
    sizes_json TEXT DEFAULT '{}',
    quantity INTEGER NOT NULL DEFAULT 0,
    received_quantity INTEGER DEFAULT 0,
    cost_per_unit REAL DEFAULT 0,
    image_url TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS sales_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER NOT NULL,
    order_id TEXT DEFAULT '',
    order_number TEXT DEFAULT '',
    customer_name TEXT DEFAULT '',
    customer_phone TEXT DEFAULT '',
    amount REAL NOT NULL DEFAULT 0,
    type TEXT DEFAULT 'online',
    channel TEXT DEFAULT 'website',
    sale_date TEXT NOT NULL,
    commission_rate REAL DEFAULT 2.5,
    incentive_amount REAL DEFAULT 0,
    incentive_paid INTEGER DEFAULT 0,
    notes TEXT DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS exchanges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT DEFAULT '',
    order_number TEXT DEFAULT '',
    customer_name TEXT NOT NULL,
    customer_phone TEXT DEFAULT '',
    reason TEXT DEFAULT '',
    original_item TEXT DEFAULT '',
    original_size TEXT DEFAULT '',
    exchange_item TEXT DEFAULT '',
    exchange_size TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    priority TEXT DEFAULT 'normal',
    assigned_to INTEGER,
    resolution_notes TEXT DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    resolved_at DATETIME
  );

  CREATE TABLE IF NOT EXISTS marketing_tracker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_product_id TEXT DEFAULT '',
    product_title TEXT NOT NULL,
    sku TEXT DEFAULT '',
    category TEXT DEFAULT '',
    raw_images_count INTEGER DEFAULT 0,
    ai_images_count INTEGER DEFAULT 0,
    website_live INTEGER DEFAULT 0,
    website_live_date TEXT DEFAULT '',
    instagram_status TEXT DEFAULT 'pending',
    instagram_date TEXT DEFAULT '',
    instagram_reel_url TEXT DEFAULT '',
    facebook_status TEXT DEFAULT 'pending',
    facebook_date TEXT DEFAULT '',
    reels_count INTEGER DEFAULT 0,
    reel_views INTEGER DEFAULT 0,
    remarketing_done INTEGER DEFAULT 0,
    notes TEXT DEFAULT '',
    uploaded_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS cod_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    awb TEXT UNIQUE NOT NULL,
    order_id TEXT DEFAULT '',
    order_number TEXT DEFAULT '',
    customer_name TEXT DEFAULT '',
    customer_phone TEXT DEFAULT '',
    city TEXT DEFAULT '',
    state TEXT DEFAULT '',
    cod_amount REAL DEFAULT 0,
    collected_date TEXT DEFAULT '',
    velocity_charges REAL DEFAULT 0,
    net_amount REAL DEFAULT 0,
    remittance_id TEXT DEFAULT '',
    remittance_date TEXT DEFAULT '',
    bank_credited INTEGER DEFAULT 0,
    bank_credit_date TEXT DEFAULT '',
    discrepancy_amount REAL DEFAULT 0,
    notes TEXT DEFAULT '',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS ndr_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    awb TEXT NOT NULL,
    order_id TEXT DEFAULT '',
    order_number TEXT DEFAULT '',
    customer_name TEXT DEFAULT '',
    customer_phone TEXT DEFAULT '',
    city TEXT DEFAULT '',
    ndr_reason TEXT DEFAULT '',
    ndr_date TEXT DEFAULT '',
    attempt_count INTEGER DEFAULT 1,
    action_taken TEXT DEFAULT '',
    action_date TEXT DEFAULT '',
    reattempt_date TEXT DEFAULT '',
    resolved INTEGER DEFAULT 0,
    resolution_type TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS shipment_cache (
    awb TEXT PRIMARY KEY,
    order_id TEXT DEFAULT '',
    order_number TEXT DEFAULT '',
    customer_name TEXT DEFAULT '',
    customer_phone TEXT DEFAULT '',
    city TEXT DEFAULT '',
    status TEXT DEFAULT '',
    velocity_status TEXT DEFAULT '',
    payment_type TEXT DEFAULT '',
    cod_amount REAL DEFAULT 0,
    weight REAL DEFAULT 0,
    pickup_date TEXT DEFAULT '',
    delivered_date TEXT DEFAULT '',
    data_json TEXT DEFAULT '{}',
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS product_images (
    variant_id TEXT PRIMARY KEY,
    product_id TEXT DEFAULT '',
    image_url TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS order_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    note TEXT NOT NULL,
    created_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

// Column migrations (safe to run on every startup)
try { db.exec("ALTER TABLE exchanges ADD COLUMN action_type TEXT DEFAULT 'exchange'"); } catch(e) {}
try { db.exec("ALTER TABLE exchanges ADD COLUMN remarks TEXT DEFAULT ''"); } catch(e) {}
try { db.exec('CREATE TABLE IF NOT EXISTS customer_names (id INTEGER PRIMARY KEY, name TEXT)'); } catch(e) {}
try { db.exec("ALTER TABLE users ADD COLUMN permissions TEXT DEFAULT '[]'"); } catch(e) {}

// Seed admin user
const uc = db.prepare('SELECT COUNT(*) as c FROM users').get();
if (uc.c === 0) {
  db.prepare('INSERT INTO users (name,email,password_hash,role,department) VALUES (?,?,?,?,?)').run(
    'Admin', 'admin@sanki.in', bcrypt.hashSync('sanki2024', 10), 'owner', 'Management'
  );
}

// ═══ MIDDLEWARE ═════════════════════════════════════════════
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || 'sanki-ops-2024-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 7 * 24 * 60 * 60 * 1000, httpOnly: true }
}));

const auth = (req, res, next) => {
  if (!req.session.userId) return res.status(401).json({ error: 'Not authenticated' });
  next();
};

// ═══ HELPERS ════════════════════════════════════════════════
const getSetting = (k) => {
  const envMap = { shopify_domain: 'SHOPIFY_DOMAIN', shopify_token: 'SHOPIFY_TOKEN' };
  if (envMap[k] && process.env[envMap[k]]) return process.env[envMap[k]];
  const r = db.prepare('SELECT value FROM settings WHERE key=?').get(k); return r ? r.value : null;
};
const setSetting = (k, v) => db.prepare('INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)').run(k, v);
const fmt = (n) => '₹' + Number(n||0).toLocaleString('en-IN');

async function shopifyFetch(endpoint, opts = {}) {
  const domain = getSetting('shopify_domain');
  const token = getSetting('shopify_token');
  if (!domain || !token) throw new Error('Shopify not configured. Please add credentials in Settings.');
  const url = `https://${domain}/admin/api/2024-01/${endpoint}`;
  const r = await fetch(url, { ...opts, headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json', ...(opts.headers||{}) } });
  if (!r.ok) { const e = await r.text(); throw new Error(`Shopify ${r.status}: ${e.substring(0,200)}`); }
  return r.json();
}

// Cursor-paginated Shopify fetch — follows Link: rel="next" until all orders retrieved (max 1000)
async function shopifyFetchAll(endpoint, maxOrders=1000) {
  const domain = getSetting('shopify_domain');
  const token = getSetting('shopify_token');
  if (!domain || !token) throw new Error('Shopify not configured. Please add credentials in Settings.');
  let allOrders = [];
  let nextUrl = `https://${domain}/admin/api/2024-01/${endpoint}`;
  while (nextUrl && allOrders.length < maxOrders) {
    const r = await fetch(nextUrl, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
    if (!r.ok) { const e = await r.text(); throw new Error(`Shopify ${r.status}: ${e.substring(0,200)}`); }
    const data = await r.json();
    allOrders = allOrders.concat(data.orders || []);
    // Shopify cursor pagination via Link header
    const link = r.headers.get('Link') || '';
    const nextMatch = link.match(/<([^>]+)>;\s*rel="next"/);
    nextUrl = nextMatch ? nextMatch[1] : null;
  }
  return { orders: allOrders };
}

// Velocity token cache
let _velToken = null;
let _velTokenExpiry = 0;

async function getVelocityToken() {
  const username = getSetting('velocity_username');
  const password = getSetting('velocity_password');
  const baseUrl = getSetting('velocity_base_url') || 'https://shazam.velocity.in/custom/api/v1';
  if (!username || !password) throw new Error('Velocity credentials not configured. Add username & password in Settings.');
  if (_velToken && Date.now() < _velTokenExpiry) return _velToken;
  // Try multiple endpoint paths and field name variants
  const authEndpoints = ['auth-token', 'auth/token', 'token', 'auth/login', 'login', 'user/login'];
  const bodies = [
    { mobile: username, password },
    { username, password },
    { email: username, password },
    { phone: username, password },
    { mobileNumber: username, password },
  ];
  let lastErr = null;
  const hdrs = { 'Content-Type': 'application/json', 'Accept': 'application/json' };
  for (const endpoint of authEndpoints) {
    for (const body of bodies) {
      try {
        const r = await fetch(`${baseUrl}/${endpoint}`, {
          method: 'POST',
          headers: hdrs,
          body: JSON.stringify(body)
        });
        const text = await r.text();
        if (text.startsWith('<')) { lastErr = `HTML response from /${endpoint}`; continue; }
        let d; try { d = JSON.parse(text); } catch(pe) { lastErr = `Invalid JSON from /${endpoint}`; continue; }
        const tok = d.token || d.access_token || d.data?.token || d.accessToken || d.authToken || d.jwt;
        if (tok) {
          _velToken = tok;
          _velTokenExpiry = Date.now() + 22 * 60 * 60 * 1000;
          return _velToken;
        }
        if (d.error || d.message) lastErr = `${endpoint}: ${d.error || d.message}`;
        else lastErr = `${endpoint}: ${JSON.stringify(d).substring(0, 100)}`;
      } catch(e) { lastErr = `${endpoint}: ${e.message}`; }
    }
  }
  throw new Error(`Velocity auth failed — tried ${authEndpoints.length} endpoints. Last error: ${lastErr}`);
}

async function velocityFetch(endpoint, opts = {}) {
  const baseUrl = getSetting('velocity_base_url') || 'https://shazam.velocity.in/custom/api/v1';
  const token = await getVelocityToken();
  const url = `${baseUrl}/${endpoint}`;
  const r = await fetch(url, {
    ...opts,
    headers: { 'Authorization': `Token ${token}`, 'Content-Type': 'application/json', ...(opts.headers||{}) }
  });
  if (!r.ok) {
    if (r.status === 401) { _velToken = null; } // reset token on auth failure
    const e = await r.text();
    throw new Error(`Velocity ${r.status}: ${e.substring(0,200)}`);
  }
  return r.json();
}

function normalizeVelStatus(s) {
  if (!s) return 'unknown';
  const l = s.toLowerCase();
  if (l.includes('delivered')) return 'delivered';
  if (l.includes('out for delivery') || l.includes('ofd')) return 'out_for_delivery';
  if (l.includes('ndr') || l.includes('non delivery') || l.includes('undelivered')) return 'ndr';
  if (l.includes('rto') || l.includes('return')) return 'rto';
  if (l.includes('pickup') && !l.includes('not')) return 'picked_up';
  if (l.includes('in transit') || l.includes('intransit')) return 'in_transit';
  if (l.includes('cancelled')) return 'cancelled';
  if (l.includes('booked') || l.includes('manifested')) return 'booked';
  return 'in_transit';
}

// ═══ AUTH ════════════════════════════════════════════════════
app.get('/api/auth/me', (req, res) => {
  if (!req.session.userId) return res.json({ user: null });
  const u = db.prepare('SELECT id,name,email,role,department,permissions FROM users WHERE id=?').get(req.session.userId);
  if (!u) return res.json({ user: null });
  res.json({ user: { ...u, permissions: JSON.parse(u.permissions||'[]') } });
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const u = db.prepare('SELECT * FROM users WHERE email=? AND active=1').get(email);
    if (!u || !bcrypt.compareSync(password, u.password_hash))
      return res.status(401).json({ error: 'Invalid email or password' });
    req.session.userId = u.id;
    res.json({ user: { id:u.id, name:u.name, email:u.email, role:u.role, department:u.department, permissions: JSON.parse(u.permissions||'[]') } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/logout', (req, res) => { req.session.destroy(); res.json({ ok: true }); });

// ═══ SETTINGS ════════════════════════════════════════════════
app.get('/api/settings', auth, (req, res) => {
  const keys = ['shopify_domain','shopify_token','velocity_username','velocity_password','velocity_base_url','company_name','currency','commission_default'];
  const out = {};
  keys.forEach(k => { out[k] = getSetting(k)||''; });
  if (out.shopify_token && out.shopify_token.length > 6) out.shopify_token_masked = out.shopify_token.substring(0,4)+'****';
  if (out.velocity_password && out.velocity_password.length > 2) out.velocity_password_masked = '****';
  res.json(out);
});

app.post('/api/settings', auth, (req, res) => {
  const allowed = ['shopify_domain','shopify_token','velocity_username','velocity_password','velocity_base_url','company_name','currency','commission_default'];
  allowed.forEach(k => { if (req.body[k] !== undefined && req.body[k] !== '****' && !String(req.body[k]).includes('****')) setSetting(k, req.body[k]); });
  // Reset cached velocity token when credentials change
  if (req.body.velocity_username || req.body.velocity_password) { _velToken = null; _velTokenExpiry = 0; }
  res.json({ ok: true });
});

// ═══ SHOPIFY PROXY ════════════════════════════════════════════
app.get('/api/shopify/orders', auth, async (req, res) => {
  try {
    const { status='any', limit=250, created_at_min, fulfillment_status, financial_status } = req.query;
    let qs = `limit=${limit}&status=${status}`;
    if (created_at_min) qs += `&created_at_min=${created_at_min}`;
    if (fulfillment_status) qs += `&fulfillment_status=${fulfillment_status}`;
    if (financial_status) qs += `&financial_status=${financial_status}`;
    const data = await shopifyFetch(`orders.json?${qs}`);
    // Enrich: use GraphQL to get real customer names (REST API strips names for POS customers)
        const orders = data.orders || [];
        const missingIds = [...new Set(
          orders.filter(o => o.customer && o.customer.id && !o.customer.first_name && !o.customer.last_name)
                .map(o => o.customer.id)
        )];
        if (missingIds.length > 0) {
          try {
            const domain = getSetting('shopify_domain');
            const token = getSetting('shopify_token');
            const gids = missingIds.map(id => 'gid://shopify/Customer/' + id);
            const gqlQuery = '{ nodes(ids: ' + JSON.stringify(gids) + ') { ... on Customer { id firstName lastName displayName } } }';
            const gqlResp = await fetch('https://' + domain + '/admin/api/2024-01/graphql.json', {
              method: 'POST',
              headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
              body: JSON.stringify({ query: gqlQuery })
            });
            const gqlData = await gqlResp.json();
            const custMap = {};
            ((gqlData.data && gqlData.data.nodes) || []).forEach(c => {
              if (c && c.id) {
                const numId = parseInt(c.id.split('/').pop(), 10);
                custMap[numId] = c;
              }
            });
            orders.forEach(o => {
              if (o.customer && o.customer.id && custMap[o.customer.id]) {
                const c = custMap[o.customer.id];
                o.customer.first_name = c.firstName || c.displayName || '';
                o.customer.last_name = c.lastName || '';
              }
            });
          } catch(_) { /* silently skip enrichment on error */ }
        }
                res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/shopify/order/:id', auth, async (req, res) => {
  try {
    const data = await shopifyFetch(`orders/${req.params.id}.json`);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/shopify/products', auth, async (req, res) => {
  try {
    const data = await shopifyFetch('products.json?limit=250');
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/shopify/customers', auth, async (req, res) => {
  try {
    const { query, limit=50 } = req.query;
    const qs = query ? `query=${encodeURIComponent(query)}&limit=${limit}` : `limit=${limit}`;
    const data = await shopifyFetch(`customers/search.json?${qs}`);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Sync product images into local cache
app.post('/api/products/sync-images', auth, async (req, res) => {
  try {
    let count = 0;
    const stmt = db.prepare('INSERT OR REPLACE INTO product_images (variant_id,product_id,image_url) VALUES (?,?,?)');
    // Fetch all products with variants and images
    let page_info = null;
    let pages = 0;
    do {
      const qs = page_info ? `limit=250&page_info=${page_info}` : 'limit=250&fields=id,variants,images';
      const data = await shopifyFetch(`products.json?${qs}`);
      const products = data.products || [];
      products.forEach(p => {
        const imgById = {};
        (p.images||[]).forEach(img => { imgById[img.id] = img.src; });
        const defaultImg = p.images?.[0]?.src || null;
        (p.variants||[]).forEach(v => {
          const url = (v.image_id ? imgById[v.image_id] : null) || defaultImg;
          if (url) { stmt.run(String(v.id), String(p.id), url); count++; }
        });
      });
      // Pagination
      page_info = data.next_page ? new URL('https://x.myshopify.com?' + data.next_page).searchParams.get('page_info') : null;
      pages++;
    } while (page_info && pages < 10);
    res.json({ ok: true, synced: count });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POS orders (channel = pos)
app.get('/api/shopify/pos-orders', auth, async (req, res) => {
  try {
    const data = await shopifyFetch('orders.json?limit=250&status=any&source_name=pos');
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Combined orders list: Shopify + Velocity status merged
app.get('/api/orders/list', auth, async (req, res) => {
  try {
    const { search, channel, action_type } = req.query;
    const from = req.query.from || '';
    const to = req.query.to || '';
    // Use limit=250 (Shopify max per page) + cursor pagination to get ALL orders in range
    let qs = `limit=250&status=any`;
    if (from) qs += `&created_at_min=${from}T00:00:00+05:30`;
    if (to) qs += `&created_at_max=${to}T23:59:59+05:30`;
    const data = await shopifyFetchAll(`orders.json?${qs}`);
    let orders = data.orders || [];

    // Collect all AWBs
    const awbSet = new Set();
    orders.forEach(o => {
      (o.fulfillments||[]).forEach(f => (f.tracking_numbers||[]).forEach(t => { if(t) awbSet.add(t); }));
      (o.note_attributes||[]).forEach(a => { if(a.name?.toLowerCase().includes('awb')&&a.value) awbSet.add(a.value); });
    });

    // Lookup shipment cache & NDR
    const statusMap = {}, ndrMap = {}, ndrOrderMap = {};
    if (awbSet.size > 0) {
      const awbArr = [...awbSet];
      const phs = awbArr.map(()=>'?').join(',');
      db.prepare(`SELECT awb,status,velocity_status,cod_amount,weight,data_json FROM shipment_cache WHERE awb IN (${phs})`).all(...awbArr).forEach(c=>{statusMap[c.awb]=c;});
      db.prepare(`SELECT awb,resolved FROM ndr_log WHERE awb IN (${phs})`).all(...awbArr).forEach(n=>{ndrMap[n.awb]=n;});
    }
    // Fix #11: also lookup NDR by order number (e.g. "#1996") for orders whose AWB isn't in ndr_log
    const orderNamesList = orders.map(o=>o.name).filter(Boolean);
    if (orderNamesList.length > 0) {
      const uniqueOrdNames = [...new Set([...orderNamesList, ...orderNamesList.map(n=>n.replace('#',''))])];
      const nOrdPhs = uniqueOrdNames.map(()=>'?').join(',');
      db.prepare(`SELECT order_number,resolved FROM ndr_log WHERE order_number IN (${nOrdPhs})`).all(...uniqueOrdNames).forEach(n=>{ ndrOrderMap[n.order_number]=n; });
    }

    // IMAGE FIX: Fetch variant-specific images from Shopify for current orders
    const imageCache = {};
    const allProductIds = [...new Set(orders.flatMap(o => (o.line_items||[]).map(i => i.product_id).filter(Boolean)))];
    if (allProductIds.length > 0) {
      try {
        for (let bi = 0; bi < allProductIds.length; bi += 50) {
          const batch = allProductIds.slice(bi, bi+50);
          const prodData = await shopifyFetch('products.json?ids=' + batch.join(',') + '&limit=50&fields=id,variants,images');
          (prodData.products || []).forEach(p => {
            const imgById = {};
            (p.images||[]).forEach(img => { imgById[img.id] = img.src; });
            (p.variants||[]).forEach(v => {
              const src = (v.image_id && imgById[v.image_id]) ? imgById[v.image_id] : (p.images&&p.images[0]&&p.images[0].src);
              if (src) imageCache[String(v.id)] = src;
            });
          });
        }
      } catch(e) {}
    }

    // FIX 2: Fetch transaction data for partially_paid orders to get actual paid amounts
    const txMap = {};
    const partialOrders = orders.filter(o => o.financial_status === 'partially_paid');
    for (const o of partialOrders) {
      try {
        const txData = await shopifyFetch(`orders/${o.id}/transactions.json?fields=amount,status,kind`);
        const txs = txData.transactions || [];
        const paid = txs.filter(t => t.status === 'success' && (t.kind === 'capture' || t.kind === 'sale'))
                        .reduce((s, t) => s + parseFloat(t.amount||0), 0);
        const refunded = txs.filter(t => t.status === 'success' && t.kind === 'refund')
                            .reduce((s, t) => s + parseFloat(t.amount||0), 0);
        txMap[o.id] = Math.max(0, paid - refunded);
      } catch(e) { txMap[o.id] = 0; }
    }
    // Note: refunded/partially_refunded orders use o.refunds[] directly — no extra API call needed

    // Build action map from exchanges table (latest action per order)
    const exchangeMap = {};
    db.prepare('SELECT order_id, action_type, exchange_item, reason FROM exchanges ORDER BY id ASC').all().forEach(e=>{ exchangeMap[String(e.order_id)]=e; });

    // CUSTOMER NAME FIX: Batch-fetch names from Shopify, cached in customer_names table
    const customerIds = [...new Set(orders.filter(o => o.customer&&o.customer.id).map(o => o.customer.id))];
    const customerMap = {};
    if (customerIds.length > 0) {
      try { const phs3 = customerIds.map(()=>'?').join(','); db.prepare('SELECT id,name FROM customer_names WHERE id IN (' + phs3 + ')').all(...customerIds).forEach(r => { customerMap[r.id] = r.name; }); } catch(e) {}
      const missing = customerIds.filter(id => !(id in customerMap));
      if (missing.length > 0) {
        try {
          for (let ci = 0; ci < missing.length; ci += 250) {
            const batch = missing.slice(ci, ci+250);
            const domain = getSetting('shopify_domain');
            const token = getSetting('shopify_token');
            const gids = batch.map(id => 'gid://shopify/Customer/' + id);
            const gqlQ = '{ nodes(ids: ' + JSON.stringify(gids) + ') { ... on Customer { id firstName lastName displayName } } }';
            const gqlR = await fetch('https://' + domain + '/admin/api/2024-01/graphql.json', {
              method: 'POST',
              headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
              body: JSON.stringify({ query: gqlQ })
            });
            const gqlData = await gqlR.json();
            const cstmt = db.prepare('INSERT OR REPLACE INTO customer_names (id,name) VALUES (?,?)');
            ((gqlData.data && gqlData.data.nodes) || []).forEach(c => { if (!c || !c.id) return; const numId = parseInt(c.id.split('/').pop(), 10); const nm = [c.firstName, c.lastName].filter(Boolean).join(' ') || c.displayName || '-'; customerMap[numId]=nm; cstmt.run(numId,nm); });
          }
        } catch(e) {}
      }
    }
    const enriched = orders.map(o => {
      const awbs = (o.fulfillments||[]).flatMap(f=>f.tracking_numbers||[]).filter(Boolean);
      const awb = awbs[0] || '';
      const isCOD = (o.payment_gateway_names||[]).some(p=>p.toLowerCase().includes('cod'))||(o.payment_gateway||'').toLowerCase().includes('cod');
      const cached = awb ? statusMap[awb] : null;
      const ndr = (awb ? ndrMap[awb] : null) || ndrOrderMap[o.name] || ndrOrderMap[(o.name||'').replace('#','')];
      const isCancelled = !!o.cancelled_at;

      // FIX 3: Better delivery status - use Velocity cache first, then Shopify's own shipment_status
      let delivery = 'pending';
      if (isCancelled) {
        delivery = 'cancelled';
      } else if (cached && cached.status) {
        // Velocity data takes priority
        delivery = cached.status;
      } else if (ndr && !ndr.resolved) {
        delivery = 'ndr';
      } else if (o.fulfillment_status === 'fulfilled') {
        // Fix #16: Walk-in (POS) customers collect in-store — use 'fulfilled', not 'dispatched'
        if ((o.source_name||'').toLowerCase() === 'pos') {
          delivery = 'fulfilled';
        } else {
          // Use Shopify's own tracking shipment_status if available
          const shipStatuses = (o.fulfillments||[]).map(f => f.shipment_status).filter(Boolean);
          if (shipStatuses.some(s => s === 'delivered')) delivery = 'delivered';
          else if (shipStatuses.some(s => s === 'out_for_delivery')) delivery = 'out_for_delivery';
          else if (shipStatuses.some(s => ['in_transit', 'confirmed', 'label_purchased'].includes(s))) delivery = 'in_transit';
          else delivery = 'dispatched';
        }
      } else if (o.fulfillment_status === 'partial') {
        delivery = 'partial';
      }
      // Fix #10: COD order marked paid in Shopify = COD collected on delivery
      if (!isCancelled && isCOD && o.financial_status === 'paid' && delivery === 'dispatched') {
        delivery = 'delivered';
      }
      // Still apply NDR from our log even if we have Velocity status (but not for cancelled orders)
      if (!isCancelled && ndr && !ndr.resolved && delivery !== 'delivered') delivery = 'ndr';

      // Channel: pos, web, draft etc.
      const src = (o.source_name||'web').toLowerCase();
      const orderChannel = src === 'pos' ? 'pos' : (src.includes('draft') ? 'draft' : 'web');

      // Courier partner from fulfillments
      const courier = [...new Set((o.fulfillments||[]).map(f=>f.tracking_company).filter(Boolean))].join(', ');

      // Payment mode from ALL gateway names (Fix #12: Paytm Machine, Fix #14: multiple sources)
      const gwNames = [...new Set([...(o.payment_gateway_names||[]), o.payment_gateway||''].filter(Boolean))];
      const gwLabel = (g) => {
        const gl = g.toLowerCase().replace(/_/g,' ');
        return gl.includes('paytm machine') || gl.includes('paytm_machine') ? 'Paytm Machine'
          : gl.includes('paytm') ? 'Paytm'
          : gl.includes('cash') && !gl.includes('cod') ? 'Cash'
          : gl.includes('upi') ? 'UPI'
          : gl.includes('razorpay') ? 'Razorpay'
          : gl.includes('phonepe') ? 'PhonePe'
          : gl.includes('gpay') || gl.includes('google pay') ? 'Google Pay'
          : gl.includes('card') || gl.includes('stripe') ? 'Card'
          : gl.includes('bank') || gl.includes('neft') || gl.includes('imps') ? 'Bank Transfer'
          : gl.includes('cod') ? 'COD'
          : gl.includes('wallet') ? 'Wallet'
          : null;
      };
      const gwModes = [...new Set(gwNames.map(gwLabel).filter(Boolean))];
      const payment_mode = gwModes.length > 0 ? gwModes.join(' + ') : (gwNames[0] ? gwNames[0].replace(/\b\w/g,c=>c.toUpperCase()) : '');

      // FIX 1 + IMAGE FIX: Line items with name + SKU + quantity + cached image
      const line_items = (o.line_items||[]).map(i => ({
        name: i.name || '',
        sku: i.sku || '',
        quantity: i.quantity || 1,
        price: parseFloat(i.price||0),
        variant_title: i.variant_title || '',
        // Use cached product image first, then Shopify inline image if any
        image: imageCache[String(i.variant_id)] || i.image?.src || null,
      }));
      const items = line_items.map(i=>`${i.name} ×${i.quantity}`).join(', ');

      // FIX 2: Payment type & correct paid/balance amounts
      const totalPrice = parseFloat(o.total_price||0);
      const isPOS = orderChannel === 'pos';
      const isPartialCOD = (isCOD || isPOS) && o.financial_status === 'partially_paid';
      let paymentType = (isCOD || (isPOS && o.financial_status !== 'paid'))
        ? (isPartialCOD ? 'Partial COD' : 'COD')
        : 'Prepaid';
      let paid_amount = 0, balance_amount = 0;

      if (o.financial_status === 'refunded') {
        // Full refund — money returned to customer
        paymentType = 'Refunded';
        paid_amount = 0; balance_amount = 0;
      } else if (o.financial_status === 'partially_refunded') {
        // Partial refund — calculate net amount still retained
        paymentType = 'Partial Refund';
        const totalRefunded = (o.refunds||[]).reduce((sum, r) => {
          const lineTotal = (r.refund_line_items||[]).reduce((s, li) => s + parseFloat(li.subtotal||0), 0);
          // Also include shipping refunds
          const shipTotal = (r.order_adjustments||[]).filter(a=>a.kind==='shipping_refund').reduce((s,a)=>s+Math.abs(parseFloat(a.amount||0)),0);
          return sum + lineTotal + shipTotal;
        }, 0);
        paid_amount = Math.max(0, totalPrice - totalRefunded);
        balance_amount = 0; // No balance to collect — customer already paid, we refunded part
      } else if (o.financial_status === 'paid') {
        // Fully paid (COD collected or prepaid)
        paid_amount = totalPrice; balance_amount = 0;
      } else if (o.financial_status === 'partially_paid') {
        // Use actual transaction data (online advance paid)
        paid_amount = txMap[o.id] !== undefined ? txMap[o.id] : 0;
        balance_amount = Math.max(0, totalPrice - paid_amount);
        // FIX 2: If Velocity confirms delivery → COD balance also collected, clear balance
        if (cached && cached.status === 'delivered') {
          paid_amount = totalPrice; balance_amount = 0;
          paymentType = 'COD (Collected)';
        }
      } else if (isCOD) {
        // FIX 5: If Velocity shows delivered for this COD, it means COD collected
        if (cached && cached.status === 'delivered') {
          paid_amount = totalPrice; balance_amount = 0;
          paymentType = 'COD (Collected)';
        } else if (o.financial_status === 'paid') {
          // Fix #10: COD marked paid in Shopify = COD was collected on delivery
          paid_amount = totalPrice; balance_amount = 0;
          paymentType = 'COD (Collected)';
        } else {
          // COD pending - balance to collect on delivery
          paid_amount = 0; balance_amount = totalPrice;
        }
      } else if (['pending', 'authorized', 'voided'].includes(o.financial_status)) {
        // Fix #13: Online order not yet paid — must not show as paid
        paid_amount = 0; balance_amount = totalPrice;
        paymentType = 'Unpaid';
      } else {
        // Prepaid - fully paid upfront
        paid_amount = totalPrice; balance_amount = 0;
      }

      // FIX 1: Package details — Velocity cache first, then Shopify order weight, then line item grams
      let package_weight = parseFloat(cached?.weight||0);
      if (!package_weight) {
        // Shopify's total_weight is in grams
        if (o.total_weight && o.total_weight > 0) {
          package_weight = parseFloat(o.total_weight) / 1000;
        } else {
          // Sum individual line item weights (grams × qty)
          const lineGrams = (o.line_items||[]).reduce((sum, i) => sum + ((parseFloat(i.grams)||0) * (i.quantity||1)), 0);
          if (lineGrams > 0) package_weight = lineGrams / 1000;
        }
      }
      package_weight = Math.round(package_weight * 100) / 100; // round to 2 decimal places
      let package_details = null;
      if (cached?.data_json) { try { package_details = JSON.parse(cached.data_json); } catch(e){} }
      let pkg_summary = '';
      if (package_weight > 0) pkg_summary = `${package_weight}kg`;
      if (package_details?.dimensions) pkg_summary += ` ${package_details.dimensions}`;
      if (package_details?.volumetric_weight) pkg_summary += ` (vol:${package_details.volumetric_weight}kg)`;

      return {
        id: o.id, name: o.name, date: (o.created_at||'').substring(0,10),
        customer: ([o.customer?.first_name, o.customer?.last_name].filter(Boolean).join(' ') || o.billing_address?.name || o.shipping_address?.name || (o.source_name==='pos'?o.note:null) || '-'),
        phone: o.billing_address?.phone || o.shipping_address?.phone || o.customer?.phone || '',
        city: o.shipping_address?.city || '',
        state: o.shipping_address?.province || '',
        line_items, items,
        amount: totalPrice,
        paid_amount, balance_amount,
        payment: paymentType,
        financial_status: o.financial_status,
        fulfillment_status: o.fulfillment_status || 'unfulfilled',
        awb, delivery,
        velocity_status: cached?.velocity_status || '',
        courier,
        channel: orderChannel,
        package_weight, pkg_summary,
        payment_mode,
        cancelled: isCancelled,
        action_type: (exchangeMap[String(o.id)]||{}).action_type||null,
        action_item: (exchangeMap[String(o.id)]||{}).exchange_item||null,
        action_reason: (exchangeMap[String(o.id)]||{}).reason||null,
      };
    });

    // Client-side filters
    const q = (search||'').toLowerCase();
    let filtered = q ? enriched.filter(o=>
      o.name.toLowerCase().includes(q)||o.customer.toLowerCase().includes(q)||
      o.phone.includes(q)||o.awb.includes(q)||o.city.toLowerCase().includes(q)||
      o.items.toLowerCase().includes(q)
    ) : enriched;
    if (channel && channel !== 'all') filtered = filtered.filter(o=>o.channel===channel);
    if (action_type && action_type !== 'all') filtered = filtered.filter(o=>o.action_type===action_type);

    res.json({ orders: filtered, total: filtered.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ VELOCITY PROXY ════════════════════════════════════════════
// Test endpoint (GET) - just verifies auth works
app.get('/api/velocity/track', auth, async (req, res) => {
  try {
    await getVelocityToken();
    res.json({ ok: true, message: 'Velocity connected!' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/velocity/track', auth, async (req, res) => {
  try {
    const { awbs } = req.body;
    const data = await velocityFetch('track', { method:'POST', body: JSON.stringify({ awb: awbs }) });
    // Cache results
    if (data && Array.isArray(data)) {
      const stmt = db.prepare('INSERT OR REPLACE INTO shipment_cache (awb,status,velocity_status,cod_amount,weight,data_json,last_updated) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)');
      data.forEach(s => {
        if (!s.awb) return;
        const normStatus = normalizeVelStatus(s.status||'');
        const codAmt = parseFloat(s.cod_amount || s.amount || 0);
        const weight = parseFloat(s.weight || s.actual_weight || 0);
        stmt.run(s.awb, normStatus, s.status||'', codAmt, weight, JSON.stringify(s));

        // FIX 5: When Velocity confirms delivery of a COD order, record it as collected
        if (normStatus === 'delivered') {
          // Mark collected date in cod_ledger if not already credited
          const codEntry = db.prepare('SELECT awb,bank_credited FROM cod_ledger WHERE awb=?').get(s.awb);
          if (codEntry && !codEntry.bank_credited) {
            db.prepare('UPDATE cod_ledger SET collected_date=COALESCE(collected_date,?) WHERE awb=?')
              .run(new Date().toISOString().substring(0,10), s.awb);
          }
        }

        // FIX 4: Also sync NDR/RTO into our log
        if (normStatus === 'ndr') {
          const exists = db.prepare('SELECT id FROM ndr_log WHERE awb=?').get(s.awb);
          if (!exists) {
            db.prepare('INSERT OR IGNORE INTO ndr_log (awb,ndr_reason,ndr_date,created_at,updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)')
              .run(s.awb, s.reason||s.ndr_reason||'', s.ndr_date||s.date||new Date().toISOString().substring(0,10));
          }
        }
        if (normStatus === 'rto') {
          // Mark as RTO resolved in NDR log if it was there
          db.prepare('UPDATE ndr_log SET resolution_type=\'rto\',resolved=1,updated_at=CURRENT_TIMESTAMP WHERE awb=? AND resolved=0').run(s.awb);
        }
      });
    }
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/velocity/ndr', auth, async (req, res) => {
  try {
    const { from_date, to_date } = req.query;
    const today = new Date().toISOString().substring(0,10);
    const data = await velocityFetch(`ndr?from_date=${from_date||today}&to_date=${to_date||today}`);
    // Upsert into ndr_log
    if (data && Array.isArray(data)) {
      const stmt = db.prepare('INSERT OR REPLACE INTO ndr_log (awb,order_number,customer_name,customer_phone,city,ndr_reason,ndr_date) VALUES (?,?,?,?,?,?,?)');
      data.forEach(n => { stmt.run(n.awb||'', n.order_no||n.ref_no||'', n.consignee||'', n.mobile||'', n.city||'', n.ndr_reason||n.reason||'', n.ndr_date||n.date||today); });
    }
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/velocity/cod', auth, async (req, res) => {
  try {
    const { from_date, to_date } = req.query;
    const today = new Date().toISOString().substring(0,10);
    const data = await velocityFetch(`cod-remittance?from_date=${from_date||today}&to_date=${to_date||today}`);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/velocity/rto', auth, async (req, res) => {
  try {
    const { from_date, to_date } = req.query;
    const today = new Date().toISOString().substring(0,10);
    const data = await velocityFetch(`rto?from_date=${from_date||today}&to_date=${to_date||today}`);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Bulk sync: pull Shopify orders + Velocity tracking together
app.post('/api/sync/orders', auth, async (req, res) => {
  try {
    const { days = 30 } = req.body;
    const from = new Date(Date.now() - days*86400000).toISOString();

    // Fetch Shopify orders
    const shopifyData = await shopifyFetch(`orders.json?limit=250&status=any&created_at_min=${from}`);
    const orders = shopifyData.orders || [];

    // Extract AWBs from order notes / tracking
    const awbs = [];
    orders.forEach(o => {
      const fulfillments = o.fulfillments || [];
      fulfillments.forEach(f => {
        (f.tracking_numbers || []).forEach(t => { if (t) awbs.push(t); });
      });
      // Also check note attributes
      (o.note_attributes || []).forEach(a => {
        if (a.name?.toLowerCase().includes('awb') || a.name?.toLowerCase().includes('tracking')) {
          if (a.value) awbs.push(a.value);
        }
      });
    });

    let velData = [];
    if (awbs.length > 0) {
      try {
        const uniqueAwbs = [...new Set(awbs)];
        velData = await velocityFetch('track', { method:'POST', body: JSON.stringify({ awb: uniqueAwbs }) });
        // Cache tracking results with full NDR/RTO sync
        if (Array.isArray(velData)) {
          const stmt = db.prepare('INSERT OR REPLACE INTO shipment_cache (awb,status,velocity_status,cod_amount,weight,data_json,last_updated) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)');
          velData.forEach(s => {
            if (!s.awb) return;
            const normStatus = normalizeVelStatus(s.status||'');
            stmt.run(s.awb, normStatus, s.status||'', parseFloat(s.cod_amount||0), parseFloat(s.weight||0), JSON.stringify(s));
            // Auto-mark COD collected on delivery
            if (normStatus === 'delivered') {
              db.prepare('UPDATE cod_ledger SET collected_date=COALESCE(collected_date,?) WHERE awb=? AND bank_credited=0')
                .run(new Date().toISOString().substring(0,10), s.awb);
            }
            // Sync NDR into ndr_log
            if (normStatus === 'ndr') {
              db.prepare('INSERT OR IGNORE INTO ndr_log (awb,ndr_reason,ndr_date,created_at,updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)')
                .run(s.awb, s.reason||s.ndr_reason||'', s.ndr_date||s.date||new Date().toISOString().substring(0,10));
            }
          });
        }
      } catch(ve) { /* Velocity track may fail - continue */ }
    }

    // FIX 3: Explicitly fetch NDR list from Velocity (last 30 days)
    let ndrCount = 0, rtoCount = 0;
    const syncFrom = new Date(Date.now() - days*86400000).toISOString().substring(0,10);
    const syncToday = new Date().toISOString().substring(0,10);
    try {
      const ndrData = await velocityFetch(`ndr?from_date=${syncFrom}&to_date=${syncToday}`);
      if (Array.isArray(ndrData) && ndrData.length > 0) {
        const ndrStmt = db.prepare('INSERT OR REPLACE INTO ndr_log (awb,order_number,customer_name,customer_phone,city,ndr_reason,ndr_date) VALUES (?,?,?,?,?,?,?)');
        ndrData.forEach(n => {
          if (!n.awb) return;
          ndrStmt.run(n.awb||'', n.order_no||n.ref_no||n.order_number||'', n.consignee||n.customer_name||'', n.mobile||n.phone||'', n.city||'', n.ndr_reason||n.reason||'', n.ndr_date||n.date||syncToday);
          // Also update shipment_cache with ndr status
          db.prepare('INSERT OR REPLACE INTO shipment_cache (awb,status,velocity_status,last_updated) VALUES (?,?,?,CURRENT_TIMESTAMP)').run(n.awb, 'ndr', n.ndr_reason||'ndr');
          ndrCount++;
        });
      }
    } catch(ve) { /* NDR fetch may fail */ }

    try {
      const rtoData = await velocityFetch(`rto?from_date=${syncFrom}&to_date=${syncToday}`);
      if (Array.isArray(rtoData) && rtoData.length > 0) {
        rtoData.forEach(r => {
          if (!r.awb) return;
          db.prepare('INSERT OR REPLACE INTO shipment_cache (awb,status,velocity_status,last_updated) VALUES (?,?,?,CURRENT_TIMESTAMP)').run(r.awb, 'rto', r.status||'rto');
          // Mark resolved in ndr_log if it was an NDR before
          db.prepare('UPDATE ndr_log SET resolution_type=\'rto\',resolved=1,updated_at=CURRENT_TIMESTAMP WHERE awb=? AND resolved=0').run(r.awb);
          rtoCount++;
        });
      }
    } catch(ve) { /* RTO fetch may fail */ }

    db.prepare('INSERT INTO settings (key,value) VALUES (\'last_sync\',?) ON CONFLICT(key) DO UPDATE SET value=?').run(new Date().toISOString(), new Date().toISOString());

    res.json({ orders_synced: orders.length, awbs_tracked: awbs.length, velocity_records: Array.isArray(velData) ? velData.length : 0, ndrs_synced: ndrCount, rtos_synced: rtoCount });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ DASHBOARD ════════════════════════════════════════════════
app.get('/api/dashboard', auth, async (req, res) => {
  try {
    const today = new Date().toISOString().substring(0,10);
    const ndrPending = db.prepare('SELECT COUNT(*) as c FROM ndr_log WHERE resolved=0').get();
    const codPending = db.prepare('SELECT COUNT(*) as c, COALESCE(SUM(cod_amount),0) as amt FROM cod_ledger WHERE bank_credited=0 AND cod_amount>0').get();
    const exchPending = db.prepare("SELECT COUNT(*) as c FROM exchanges WHERE status='pending'").get();
    const poPending = db.prepare("SELECT COUNT(*) as c FROM purchase_orders WHERE status NOT IN ('received','cancelled')").all();
    const lastSync = getSetting('last_sync');
    res.json({
      ndr_pending: ndrPending?.c || 0,
      cod_pending_count: codPending?.c || 0,
      cod_pending_amount: codPending?.amt || 0,
      exchanges_pending: exchPending?.c || 0,
      pos_in_transit: poPending?.length || 0,
      last_sync: lastSync
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ PURCHASE ORDERS ══════════════════════════════════════════
app.get('/api/po', auth, (req, res) => {
  const { status } = req.query;
  let q = 'SELECT po.*, (SELECT COUNT(*) FROM po_items WHERE po_id=po.id) as item_count FROM purchase_orders po WHERE 1=1';
  const params = [];
  if (status) { q += ' AND po.status=?'; params.push(status); }
  q += ' ORDER BY po.created_at DESC';
  res.json(db.prepare(q).all(...params));
});

app.post('/api/po', auth, (req, res) => {
  try {
    const { vendor_name, vendor_contact, vendor_email, order_date, expected_arrival, notes, items=[], currency='CNY', inr_rate=11.5, courier, tracking_number } = req.body;
    const po_number = 'PO-' + new Date().getFullYear() + '-' + String(Date.now()).slice(-5);
    const info = db.prepare('INSERT INTO purchase_orders (po_number,vendor_name,vendor_contact,vendor_email,order_date,expected_arrival,notes,currency,inr_rate,courier,tracking_number,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)').run(
      po_number,vendor_name,vendor_contact||'',vendor_email||'',order_date,expected_arrival||'',notes||'',currency,inr_rate,courier||'',tracking_number||'',req.session.userId
    );
    let total = 0;
    if (items.length) {
      const s = db.prepare('INSERT INTO po_items (po_id,product_name,sku,color,sizes_json,quantity,cost_per_unit,notes) VALUES (?,?,?,?,?,?,?,?)');
      items.forEach(i => { s.run(info.lastInsertRowid,i.product_name,i.sku||'',i.color||'',JSON.stringify(i.sizes||{}),i.quantity||0,i.cost_per_unit||0,i.notes||''); total += (i.quantity||0)*(i.cost_per_unit||0); });
      db.prepare('UPDATE purchase_orders SET total_amount=? WHERE id=?').run(total, info.lastInsertRowid);
    }
    res.json({ id:info.lastInsertRowid, po_number });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/po/:id', auth, (req, res) => {
  const po = db.prepare('SELECT * FROM purchase_orders WHERE id=?').get(req.params.id);
  if (!po) return res.status(404).json({ error: 'Not found' });
  po.items = db.prepare('SELECT * FROM po_items WHERE po_id=? ORDER BY id').all(req.params.id);
  res.json(po);
});

app.put('/api/po/:id', auth, (req, res) => {
  try {
    const { status, actual_arrival, tracking_number, courier, notes } = req.body;
    db.prepare('UPDATE purchase_orders SET status=COALESCE(?,status), actual_arrival=COALESCE(?,actual_arrival), tracking_number=COALESCE(?,tracking_number), courier=COALESCE(?,courier), notes=COALESCE(?,notes) WHERE id=?').run(
      status||null,actual_arrival||null,tracking_number||null,courier||null,notes||null,req.params.id
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/po/:id/receive', auth, (req, res) => {
  try {
    const { items } = req.body;
    const s = db.prepare('UPDATE po_items SET received_quantity=? WHERE id=?');
    items.forEach(i => s.run(i.received_quantity||0, i.id));
    const all = db.prepare('SELECT * FROM po_items WHERE po_id=?').all(req.params.id);
    const done = all.every(i => (i.received_quantity||0) >= i.quantity);
    const partial = all.some(i => (i.received_quantity||0) > 0);
    const newStatus = done ? 'received' : partial ? 'partial_receipt' : 'sent';
    const arrival = (done||partial) ? new Date().toISOString().substring(0,10) : null;
    db.prepare('UPDATE purchase_orders SET status=?, actual_arrival=COALESCE(actual_arrival,COALESCE(?,actual_arrival)) WHERE id=?').run(newStatus,arrival,req.params.id);
    res.json({ ok:true, status:newStatus });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ SALES & AGENTS ═══════════════════════════════════════════
app.get('/api/users', auth, (req, res) => {
  const rows = db.prepare('SELECT id,name,email,role,department,phone,active,permissions FROM users ORDER BY name').all();
  res.json(rows.map(u => ({ ...u, permissions: JSON.parse(u.permissions||'[]') })));
});

app.post('/api/users', auth, (req, res) => {
  try {
    const { name,email,password,role,department,phone,permissions } = req.body;
    const permsJson = JSON.stringify(Array.isArray(permissions) ? permissions : []);
    const info = db.prepare('INSERT INTO users (name,email,password_hash,role,department,phone,permissions) VALUES (?,?,?,?,?,?,?)').run(
      name,email,bcrypt.hashSync(password,10),role||'staff',department||'',phone||'',permsJson
    );
    res.json({ id:info.lastInsertRowid });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

app.put('/api/users/:id', auth, (req, res) => {
  try {
    const { name,role,department,phone,active,password,permissions } = req.body;
    const permsJson = JSON.stringify(Array.isArray(permissions) ? permissions : []);
    if (password) {
      db.prepare('UPDATE users SET name=?,role=?,department=?,phone=?,active=?,password_hash=?,permissions=? WHERE id=?').run(
        name,role,department||'',phone||'',active!=null?active:1,bcrypt.hashSync(password,10),permsJson,req.params.id
      );
    } else {
      db.prepare('UPDATE users SET name=?,role=?,department=?,phone=?,active=?,permissions=? WHERE id=?').run(
        name,role,department||'',phone||'',active!=null?active:1,permsJson,req.params.id
      );
    }
    res.json({ ok:true });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

app.get('/api/sales', auth, (req, res) => {
  const { from, to, agent_id, channel } = req.query;
  let q = 'SELECT s.*,u.name as agent_name FROM sales_records s LEFT JOIN users u ON s.agent_id=u.id WHERE 1=1';
  const p = [];
  if (from) { q+=' AND s.sale_date>=?'; p.push(from); }
  if (to) { q+=' AND s.sale_date<=?'; p.push(to); }
  if (agent_id) { q+=' AND s.agent_id=?'; p.push(agent_id); }
  if (channel) { q+=' AND s.channel=?'; p.push(channel); }
  q+=' ORDER BY s.sale_date DESC,s.id DESC';
  res.json(db.prepare(q).all(...p));
});

app.post('/api/sales', auth, (req, res) => {
  try {
    const { agent_id,order_id,order_number,customer_name,customer_phone,amount,type,channel,sale_date,commission_rate,notes } = req.body;
    const rate = commission_rate || parseFloat(getSetting('commission_default')||'2.5');
    const incentive = (parseFloat(amount)*rate)/100;
    const info = db.prepare('INSERT INTO sales_records (agent_id,order_id,order_number,customer_name,customer_phone,amount,type,channel,sale_date,commission_rate,incentive_amount,notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)').run(
      agent_id,order_id||'',order_number||'',customer_name||'',customer_phone||'',amount,type||'online',channel||'website',sale_date,rate,incentive,notes||''
    );
    res.json({ id:info.lastInsertRowid, incentive });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/sales/:id/paid', auth, (req, res) => {
  db.prepare('UPDATE sales_records SET incentive_paid=1 WHERE id=?').run(req.params.id);
  res.json({ ok:true });
});

app.get('/api/sales/summary', auth, (req, res) => {
  const { from, to } = req.query;
  const p = [from||'2024-01-01', to||new Date().toISOString().substring(0,10)];
  const rows = db.prepare(`
    SELECT u.id,u.name,u.department,
      COUNT(s.id) as orders, COALESCE(SUM(s.amount),0) as total_sales,
      COALESCE(SUM(s.incentive_amount),0) as total_incentive,
      COALESCE(SUM(CASE WHEN s.incentive_paid=1 THEN s.incentive_amount ELSE 0 END),0) as paid_incentive,
      COALESCE(SUM(CASE WHEN s.incentive_paid=0 THEN s.incentive_amount ELSE 0 END),0) as pending_incentive
    FROM users u
    LEFT JOIN sales_records s ON s.agent_id=u.id AND s.sale_date BETWEEN ? AND ?
    WHERE u.active=1 AND u.role NOT IN ('owner')
    GROUP BY u.id ORDER BY total_sales DESC
  `).all(...p);
  res.json(rows);
});

// ═══ EXCHANGES ════════════════════════════════════════════════
app.get('/api/exchanges', auth, (req, res) => {
  const { status } = req.query;
  let q = 'SELECT e.*,u.name as agent_name FROM exchanges e LEFT JOIN users u ON e.assigned_to=u.id WHERE 1=1';
  const p = [];
  if (status) { q+=' AND e.status=?'; p.push(status); }
  q+=' ORDER BY e.created_at DESC';
  res.json(db.prepare(q).all(...p));
});

app.post('/api/exchanges', auth, (req, res) => {
  try {
    const d = req.body;
    const info = db.prepare('INSERT INTO exchanges (order_id,order_number,customer_name,customer_phone,reason,original_item,original_size,exchange_item,exchange_size,priority,notes,action_type,remarks) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)').run(
      d.order_id||'',d.order_number||'',d.customer_name||'',d.customer_phone||'',d.reason||'',d.original_item||'',d.original_size||'',d.exchange_item||'',d.exchange_size||'',d.priority||'normal',d.notes||'',d.action_type||'exchange',d.remarks||''
    );
    res.json({ id:info.lastInsertRowid });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/exchanges/:id', auth, (req, res) => {
  try {
    const { status,resolution_notes,assigned_to } = req.body;
    const ra = status==='resolved' ? new Date().toISOString() : null;
    db.prepare('UPDATE exchanges SET status=COALESCE(?,status),resolution_notes=COALESCE(?,resolution_notes),assigned_to=COALESCE(?,assigned_to),resolved_at=COALESCE(resolved_at,?) WHERE id=?').run(
      status||null,resolution_notes||null,assigned_to||null,ra,req.params.id
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ MARKETING ════════════════════════════════════════════════
app.get('/api/marketing', auth, (req, res) => {
  const { ig_status, category } = req.query;
  let q = 'SELECT * FROM marketing_tracker WHERE 1=1';
  const p = [];
  if (ig_status) { q+=' AND instagram_status=?'; p.push(ig_status); }
  if (category) { q+=' AND category=?'; p.push(category); }
  q+=' ORDER BY created_at DESC';
  res.json(db.prepare(q).all(...p));
});

app.post('/api/marketing', auth, (req, res) => {
  try {
    const d = req.body;
    const info = db.prepare('INSERT INTO marketing_tracker (shopify_product_id,product_title,sku,category,raw_images_count,ai_images_count,website_live,instagram_status,facebook_status,notes,uploaded_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(
      d.shopify_product_id||'',d.product_title,d.sku||'',d.category||'',d.raw_images_count||0,d.ai_images_count||0,d.website_live||0,d.instagram_status||'pending',d.facebook_status||'pending',d.notes||'',req.session.userId
    );
    res.json({ id:info.lastInsertRowid });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/marketing/:id', auth, (req, res) => {
  try {
    const d = req.body;
    db.prepare('UPDATE marketing_tracker SET instagram_status=COALESCE(?,instagram_status),instagram_date=COALESCE(?,instagram_date),facebook_status=COALESCE(?,facebook_status),facebook_date=COALESCE(?,facebook_date),website_live=COALESCE(?,website_live),ai_images_count=COALESCE(?,ai_images_count),raw_images_count=COALESCE(?,raw_images_count),reels_count=COALESCE(?,reels_count),remarketing_done=COALESCE(?,remarketing_done),notes=COALESCE(?,notes),updated_at=CURRENT_TIMESTAMP WHERE id=?').run(
      d.instagram_status||null,d.instagram_date||null,d.facebook_status||null,d.facebook_date||null,
      d.website_live!=null?d.website_live:null,d.ai_images_count||null,d.raw_images_count||null,
      d.reels_count||null,d.remarketing_done!=null?d.remarketing_done:null,d.notes||null,req.params.id
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ COD LEDGER ═══════════════════════════════════════════════
app.get('/api/cod', auth, (req, res) => {
  const { from, to, status } = req.query;
  let q = 'SELECT * FROM cod_ledger WHERE 1=1';
  const p = [];
  if (from) { q+=' AND DATE(updated_at)>=?'; p.push(from); }
  if (to) { q+=' AND DATE(updated_at)<=?'; p.push(to); }
  if (status==='pending') q+=' AND bank_credited=0 AND cod_amount>0';
  if (status==='credited') q+=' AND bank_credited=1';
  q+=' ORDER BY updated_at DESC';
  res.json(db.prepare(q).all(...p));
});

app.post('/api/cod', auth, (req, res) => {
  try {
    const d = req.body;
    db.prepare('INSERT OR REPLACE INTO cod_ledger (awb,order_id,order_number,customer_name,customer_phone,city,cod_amount,collected_date,velocity_charges,net_amount,remittance_id,remittance_date,bank_credited,bank_credit_date,notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(
      d.awb,d.order_id||'',d.order_number||'',d.customer_name||'',d.customer_phone||'',d.city||'',
      d.cod_amount||0,d.collected_date||'',d.velocity_charges||0,d.net_amount||0,
      d.remittance_id||'',d.remittance_date||'',d.bank_credited||0,d.bank_credit_date||'',d.notes||''
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/cod/:awb/mark-credited', auth, (req, res) => {
  const { bank_credit_date } = req.body;
  db.prepare('UPDATE cod_ledger SET bank_credited=1,bank_credit_date=? WHERE awb=?').run(bank_credit_date||new Date().toISOString().substring(0,10), req.params.awb);
  res.json({ ok:true });
});

// ═══ NDR MANAGEMENT ═══════════════════════════════════════════
app.get('/api/ndr', auth, (req, res) => {
  const { resolved } = req.query;
  let q = 'SELECT * FROM ndr_log WHERE 1=1';
  if (resolved !== undefined) q += ` AND resolved=${resolved==='true'?1:0}`;
  q += ' ORDER BY created_at DESC';
  res.json(db.prepare(q).all());
});

app.put('/api/ndr/:id', auth, (req, res) => {
  try {
    const { action_taken,reattempt_date,resolved,resolution_type,notes } = req.body;
    db.prepare('UPDATE ndr_log SET action_taken=COALESCE(?,action_taken),action_date=COALESCE(action_date,DATE(CURRENT_TIMESTAMP)),reattempt_date=COALESCE(?,reattempt_date),resolved=COALESCE(?,resolved),resolution_type=COALESCE(?,resolution_type),notes=COALESCE(?,notes),updated_at=CURRENT_TIMESTAMP WHERE id=?').run(
      action_taken||null,reattempt_date||null,resolved!=null?resolved:null,resolution_type||null,notes||null,req.params.id
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ ORDER NOTES ══════════════════════════════════════════════
app.get('/api/orders/:order_id/notes', auth, (req, res) => {
  const notes = db.prepare('SELECT n.*,u.name as user_name FROM order_notes n LEFT JOIN users u ON n.created_by=u.id WHERE n.order_id=? ORDER BY n.created_at DESC').all(req.params.order_id);
  res.json(notes);
});

app.post('/api/orders/:order_id/notes', auth, (req, res) => {
  try {
    const { note } = req.body;
    const info = db.prepare('INSERT INTO order_notes (order_id,note,created_by) VALUES (?,?,?)').run(req.params.order_id, note, req.session.userId);
    res.json({ id:info.lastInsertRowid });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══ EXPORT CSV ════════════════════════════════════════════════
app.get('/api/export/orders', auth, async (req, res) => {
  try {
    const { from, to } = req.query;
    let qs = `limit=250&status=any`;
    if (from) qs += `&created_at_min=${from}T00:00:00+05:30`;
    if (to) qs += `&created_at_max=${to}T23:59:59+05:30`;
    const data = await shopifyFetchAll(`orders.json?${qs}`);
    const orders = data.orders || [];
    const esc = v => '"' + String(v||'').replace(/"/g,'""') + '"';
    const headers = ['Order#','Date','Customer','Phone','City','State','Pincode','Items','SKUs','Qty','Amount','Paid','Balance','Payment','Payment Mode','Financial Status','Delivery Status','AWB','Courier','Channel','Notes'];
    const rows = [headers.map(esc).join(',')];
    orders.forEach(o => {
      const items = (o.line_items||[]).map(i => `${i.name} x${i.quantity}`).join('; ');
      const skus = (o.line_items||[]).map(i => i.sku||'').filter(Boolean).join('; ');
      const qty = (o.line_items||[]).reduce((s,i)=>s+(i.quantity||1),0);
      const awb = (o.fulfillments||[]).flatMap(f => f.tracking_numbers||[]).join('; ');
      const courier = [...new Set((o.fulfillments||[]).map(f=>f.tracking_company).filter(Boolean))].join(', ');
      const isCOD = (o.payment_gateway_names||[]).some(p=>p.toLowerCase().includes('cod'))||(o.payment_gateway||'').toLowerCase().includes('cod');
      const src = (o.source_name||'web').toLowerCase();
      const channel = src === 'pos' ? 'POS' : 'Web';
      const totalPrice = parseFloat(o.total_price||0);
      const paidAmt = o.financial_status === 'paid' ? totalPrice : o.financial_status === 'partially_paid' ? parseFloat(o.subtotal_price||0) : 0;
      const balAmt = Math.max(0, totalPrice - paidAmt);
      const gw = ((o.payment_gateway_names||[])[0] || o.payment_gateway || '').toLowerCase();
      const payment_mode = gw.includes('upi') ? 'UPI' : gw.includes('razorpay') ? 'Razorpay' : gw.includes('paytm') ? 'Paytm' : gw.includes('cash')&&!gw.includes('cod') ? 'Cash' : gw.includes('card') ? 'Card' : gw ? gw.replace(/\b\w/g,c=>c.toUpperCase()) : '';
      const isCancelled = !!o.cancelled_at;
      const deliveryStatus = isCancelled ? 'Cancelled' : o.fulfillment_status === 'fulfilled' ? 'Fulfilled' : o.fulfillment_status || 'Unfulfilled';
      const payment = isCOD ? (o.financial_status === 'partially_paid' ? 'Partial COD' : 'COD') : 'Prepaid';
      rows.push([
        esc(o.name), esc((o.created_at||'').substring(0,10)),
        esc(o.billing_address?.name||o.customer?.first_name||''), esc(o.billing_address?.phone||o.customer?.phone||''),
        esc(o.shipping_address?.city||''), esc(o.shipping_address?.province||''), esc(o.shipping_address?.zip||''),
        esc(items), esc(skus), qty,
        totalPrice.toFixed(2), paidAmt.toFixed(2), balAmt.toFixed(2),
        esc(payment), esc(payment_mode), esc(o.financial_status||''),
        esc(deliveryStatus), esc(awb), esc(courier), esc(channel), esc(o.note||'')
      ].join(','));
    });
    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition',`attachment; filename=orders_${from||'all'}_${to||'all'}.csv`);
    res.send(rows.join('\n'));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.listen(PORT, () => console.log(`SANKI OPS running on port ${PORT}`));
