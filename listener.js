// listener.js
require('dotenv').config();
const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');

/**
 * ENV attendus (.env) :
 * WSS_URL=wss://testnet.dplabs-internal.com
 * CONTRACT=0x9a88d07850723267db386c681646217af7e220d7
 * SUPABASE_URL=...
 * SUPABASE_KEY=...           // ⚠️ côté serveur, utilise de préférence la clé "service_role"
 * DECIMALS=6                 // affichage et agrégation en ×10^-6 par défaut
 */

const WSS_URL      = process.env.WSS_URL || 'wss://testnet.dplabs-internal.com';
const CONTRACT     = (process.env.CONTRACT || '').trim();
const SUPABASE_URL = (process.env.SUPABASE_URL || '').trim();
const SUPABASE_KEY = (process.env.SUPABASE_KEY || '').trim();
const DECIMALS     = parseInt(process.env.DECIMALS || '6', 10);

if (!CONTRACT) {
  console.error('Missing CONTRACT in .env');
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_KEY in .env');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const ABI = [
  'event MarginSettled(address indexed trader,uint256 openMargin,uint256 closeMargin,uint256 profit,bool traderWon)'
];

let provider = null;
let contract = null;
const seen = new Set(); // dédoublonnage (txHash:logIndex)

/** Purge simple du Set pour éviter de grossir à l'infini */
function gcSeen(max = 50000) {
  if (seen.size <= max) return;
  // stratégie simple : clear complet (tu peux faire mieux avec une queue si besoin)
  seen.clear();
}

function formatSignedDiffX(closeBn, openBn, decimals = 6) {
  const c = ethers.BigNumber.from(closeBn);
  const o = ethers.BigNumber.from(openBn);
  if (c.gte(o)) {
    const abs = c.sub(o);
    return ethers.utils.formatUnits(abs, decimals); // ex: "12.345678"
  } else {
    const abs = o.sub(c);
    return '-' + ethers.utils.formatUnits(abs, decimals); // ex: "-0.100000"
  }
}

async function pushToSupabase(trader, deltaStr) {
  try {
    const { data, error } = await supabase.rpc('add_pnl', {
      p_trader: trader.toLowerCase(),
      p_delta_x6: deltaStr
    });
    if (error) {
      console.error('Supabase add_pnl error:', error.message || error);
      return null;
    }
    return data; // nouveau total retourné par la fonction
  } catch (e) {
    console.error('Supabase RPC catch:', e.message || e);
    return null;
  }
}

function attachListeners() {
  if (!provider) {
    provider = new ethers.providers.WebSocketProvider(WSS_URL);
    provider._websocket.on('open', () => {
      console.log('🔌 WSS connected');
    });
    provider._websocket.on('close', (code) => {
      console.warn('🔌 WSS closed:', code);
      // tentative de reconnexion après 2s
      setTimeout(reconnect, 2000);
    });
    provider._websocket.on('error', (err) => {
      console.warn('⚠️ WSS error:', err?.message || err);
    });
  }

  contract = new ethers.Contract(CONTRACT, ABI, provider);
  contract.removeAllListeners('MarginSettled');

  contract.on('MarginSettled', async (trader, openMargin, closeMargin, profit, traderWon, ev) => {
    const uid = `${ev.transactionHash}:${ev.logIndex}`;
    if (seen.has(uid)) return;
    seen.add(uid);
    gcSeen();

    const delta = formatSignedDiffX(closeMargin, openMargin, DECIMALS);
    const total = await pushToSupabase(trader, delta);
    if (total !== null) {
      console.log(
        `✅ ${trader} | ΔPnL×10^-${DECIMALS}=${delta} | total=${total} | block=${ev.blockNumber}`
      );
    }
  });

  // debug blocks (optionnel)
  provider.on('block', (bn) => {
    // console.log('⛓️ new block', bn);
  });
}

function disconnect() {
  try {
    if (contract) contract.removeAllListeners('MarginSettled');
  } catch {}
  try {
    if (provider?._websocket?.readyState === 1) {
      provider._websocket.close(1000, 'manual close');
    }
  } catch {}
  provider = null;
  contract = null;
}

function reconnect() {
  disconnect();
  attachListeners();
}

// start
attachListeners();

// graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down…');
  disconnect();
  process.exit(0);
});

