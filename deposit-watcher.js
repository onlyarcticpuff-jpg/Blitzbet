/**
 * Prophetly — Solana Deposit Watcher
 * ─────────────────────────────────────────────────────────────────────────────
 * Polls every 15 seconds. For each user in profiles that has a deposit_wallet:
 *   1. Fetches recent signatures from the Solana blockchain
 *   2. Checks which sigs are new (not already in `deposits` table)
 *   3. Parses the transaction to get the SOL amount sent
 *   4. Converts SOL → USD using live price
 *   5. Credits `profiles.balance` and inserts a record into `deposits`
 *
 * ── Setup ────────────────────────────────────────────────────────────────────
 *   npm install @solana/web3.js @supabase/supabase-js node-fetch dotenv
 *
 * ── .env ─────────────────────────────────────────────────────────────────────
 *   SUPABASE_URL=https://xxxx.supabase.co
 *   SUPABASE_SERVICE_KEY=service_role_key_here   ← NOT the anon key
 *   SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
 *   POLL_INTERVAL_MS=15000
 *
 * ── Run ──────────────────────────────────────────────────────────────────────
 *   node deposit-watcher.js
 *
 * ── Supabase SQL (run once) ──────────────────────────────────────────────────
 *   -- deposits table
 *   create table if not exists deposits (
 *     id           uuid primary key default gen_random_uuid(),
 *     user_id      uuid not null references auth.users(id),
 *     tx_signature text not null unique,
 *     amount_sol   numeric(20,9) not null,
 *     amount_usd   numeric(20,2) not null,
 *     wallet       text not null,
 *     created_at   timestamptz default now()
 *   );
 *   create index on deposits(user_id);
 *   create index on deposits(tx_signature);
 *
 *   -- balance column on profiles (add if missing)
 *   alter table profiles add column if not exists balance numeric(20,2) default 0;
 * ─────────────────────────────────────────────────────────────────────────────
 */

require("dotenv").config();

const {
  Connection,
  PublicKey,
  LAMPORTS_PER_SOL,
} = require("@solana/web3.js");

const { createClient } = require("@supabase/supabase-js");

// ── Config ────────────────────────────────────────────────────────────────────

const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_KEY      = process.env.SUPABASE_SERVICE_KEY; // service role — bypass RLS
const RPC_URL           = process.env.SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com";
const POLL_INTERVAL_MS  = Number(process.env.POLL_INTERVAL_MS) || 15_000;
const SIGNATURES_LIMIT  = 10; // how many recent sigs to check per wallet per poll

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌  Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env");
  process.exit(1);
}

// ── Clients ───────────────────────────────────────────────────────────────────

const supabase   = createClient(SUPABASE_URL, SUPABASE_KEY);
const connection = new Connection(RPC_URL, "confirmed");

// ── SOL price cache (refreshed every 60s) ────────────────────────────────────

let cachedSolPrice    = null;
let solPriceFetchedAt = 0;

async function getSolPriceUSD() {
  const now = Date.now();
  if (cachedSolPrice && now - solPriceFetchedAt < 60_000) return cachedSolPrice;

  try {
    // CoinGecko free tier — no API key needed
    const res  = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
    const json = await res.json();
    cachedSolPrice    = json?.solana?.usd ?? cachedSolPrice ?? 150;
    solPriceFetchedAt = now;
    console.log(`💰  SOL price updated: $${cachedSolPrice}`);
  } catch (err) {
    console.warn("⚠️  Could not fetch SOL price, using last known:", cachedSolPrice ?? 150);
    cachedSolPrice = cachedSolPrice ?? 150;
  }

  return cachedSolPrice;
}

// ── Fetch all profiles that have a deposit_wallet ─────────────────────────────

async function fetchWalletProfiles() {
  const { data, error } = await supabase
    .from("profiles")
    .select("id, deposit_wallet, balance")
    .not("deposit_wallet", "is", null);

  if (error) {
    console.error("❌  Failed to fetch profiles:", error.message);
    return [];
  }

  return data || [];
}

// ── Check if a signature has already been processed ──────────────────────────

async function isSignatureKnown(signature) {
  const { data, error } = await supabase
    .from("deposits")
    .select("id")
    .eq("tx_signature", signature)
    .maybeSingle();

  if (error) {
    console.error("❌  Error checking signature:", error.message);
    return true; // treat as known to avoid double-credit on error
  }

  return !!data;
}

// ── Parse a confirmed transaction and extract SOL received by `walletAddress` ─

async function getReceivedLamports(signature, walletAddress) {
  let tx;
  try {
    tx = await connection.getTransaction(signature, {
      maxSupportedTransactionVersion: 0,
    });
  } catch (err) {
    console.warn(`⚠️  Could not fetch tx ${signature.slice(0, 8)}…:`, err.message);
    return 0;
  }

  if (!tx || tx.meta?.err) return 0; // failed transaction

  const { accountKeys } = tx.transaction.message;

  // accountKeys can be a plain array or a MessageAccountKeys object
  const keys = typeof accountKeys.staticAccountKeys !== "undefined"
    ? [
        ...accountKeys.staticAccountKeys,
        ...(accountKeys.loadedAddresses?.writable  || []),
        ...(accountKeys.loadedAddresses?.readonly  || []),
      ]
    : accountKeys;

  const idx = keys.findIndex(k => k.toBase58() === walletAddress);
  if (idx === -1) return 0;

  const pre  = tx.meta.preBalances[idx]  ?? 0;
  const post = tx.meta.postBalances[idx] ?? 0;
  const diff = post - pre; // positive = received

  return diff > 0 ? diff : 0;
}

// ── Credit the user's balance and record the deposit ─────────────────────────

async function recordDeposit({ userId, walletAddress, signature, lamports, solPrice }) {
  const amountSol = lamports / LAMPORTS_PER_SOL;
  const amountUsd = parseFloat((amountSol * solPrice).toFixed(2));

  console.log(
    `💸  New deposit detected!`,
    `\n    User:      ${userId}`,
    `\n    Wallet:    ${walletAddress}`,
    `\n    Sig:       ${signature.slice(0, 16)}…`,
    `\n    Amount:    ${amountSol.toFixed(6)} SOL  ≈  $${amountUsd} USD`,
  );

  // 1. Insert deposit record (unique on tx_signature — prevents race conditions)
  const { error: insertError } = await supabase
    .from("deposits")
    .insert({
      user_id:       userId,
      tx_signature:  signature,
      amount_sol:    amountSol,
      amount_usd:    amountUsd,
      wallet:        walletAddress,
    });

  if (insertError) {
    // Could be a unique violation (race between two watcher instances) — safe to ignore
    if (insertError.code === "23505") {
      console.log(`  ↳ Duplicate sig — already recorded. Skipping.`);
      return;
    }
    console.error("❌  Failed to insert deposit:", insertError.message);
    return;
  }

  // 2. Atomically increment balance using Supabase RPC (avoids read-modify-write race)
  const { error: rpcError } = await supabase.rpc("increment_balance", {
    p_user_id: userId,
    p_amount:  amountUsd,
  });

  if (rpcError) {
    console.error("❌  Failed to credit balance:", rpcError.message);
    // NOTE: deposit is already recorded — balance can be reconciled manually
    // To auto-fix: compute SUM(amount_usd) from deposits and set balance accordingly
  } else {
    console.log(`  ✅  Balance credited $${amountUsd} to user ${userId}`);
  }
}

// ── Process one wallet ────────────────────────────────────────────────────────

async function processWallet(profile) {
  const { id: userId, deposit_wallet: walletAddress } = profile;

  let pubkey;
  try {
    pubkey = new PublicKey(walletAddress);
  } catch {
    console.warn(`⚠️  Invalid wallet address for user ${userId}: ${walletAddress}`);
    return;
  }

  // Fetch the N most recent confirmed signatures for this address
  let signatures;
  try {
    signatures = await connection.getSignaturesForAddress(pubkey, {
      limit: SIGNATURES_LIMIT,
    });
  } catch (err) {
    console.warn(`⚠️  Could not fetch signatures for ${walletAddress.slice(0, 8)}…:`, err.message);
    return;
  }

  if (!signatures || signatures.length === 0) return;

  const solPrice = await getSolPriceUSD();

  for (const sigInfo of signatures) {
    if (sigInfo.err) continue; // skip failed on-chain txns

    const { signature } = sigInfo;

    // Skip if we already processed this tx
    if (await isSignatureKnown(signature)) continue;

    const lamports = await getReceivedLamports(signature, walletAddress);
    if (lamports <= 0) continue;

    await recordDeposit({ userId, walletAddress, signature, lamports, solPrice });
  }
}

// ── Main poll loop ────────────────────────────────────────────────────────────

async function poll() {
  console.log(`\n🔍  [${new Date().toISOString()}] Polling deposits…`);

  const profiles = await fetchWalletProfiles();
  if (profiles.length === 0) {
    console.log("   No wallets found.");
    return;
  }

  console.log(`   Checking ${profiles.length} wallet(s)…`);

  // Process all wallets concurrently (but cap to avoid overwhelming RPC)
  const CONCURRENCY = 5;
  for (let i = 0; i < profiles.length; i += CONCURRENCY) {
    const batch = profiles.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(processWallet));
  }
}

// ── Boot ──────────────────────────────────────────────────────────────────────

(async () => {
  console.log("🚀  Prophetly deposit watcher starting…");
  console.log(`   RPC:      ${RPC_URL}`);
  console.log(`   Interval: ${POLL_INTERVAL_MS / 1000}s`);

  // Warm up SOL price cache
  await getSolPriceUSD();

  // Run immediately on start, then on interval
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
})();
