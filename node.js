require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { GraphQLClient, gql } = require('graphql-request');
const csv = require('fast-csv');

const DEFAULT_URLS = [
  process.env.SUBGRAPH_URL?.trim(),
  'https://api.thegraph.com/subgraphs/id/A1BC1hzDsK4NTeXBpKQnDBphngpYZAwDUF7dEBfa3jHK', // PCS v3 BSC (Messari schema)
  'https://api.thegraph.com/subgraphs/id/78EUqzJmEVJsAKvWghn7qotf9LVGqcTQxJhT5z84ZmgJ', // PCS v3 BSC (alt)
].filter(Boolean);

const WALLET_FILE = process.env.WALLET_FILE || 'account.txt';

// ---- Helpers CLI ----
function getArg(name, def = undefined) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.split('=')[1] : def;
}

function parseAddressList(str) {
  return (str || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

function uniqLower(arr) {
  const seen = new Set();
  const out = [];
  for (const x of arr) {
    const v = x.toLowerCase();
    if (!seen.has(v)) { seen.add(v); out.push(v); }
  }
  return out;
}

function utcDayBounds(isoDate) {
  // isoDate: "YYYY-MM-DD" (UTC)
  const start = Math.floor(Date.parse(isoDate + 'T00:00:00Z') / 1000);
  const end = Math.floor(Date.parse(isoDate + 'T23:59:59Z') / 1000);
  return { start, end };
}

function humanUSD(x) {
  return Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(x);
}

// ---- Load addresses ----
function loadWalletsFromFile(file) {
  try {
    const raw = fs.readFileSync(path.resolve(file), 'utf8');
    return uniqLower(
      raw.split(/\r?\n/).map(l => l.trim()).filter(Boolean)
    );
  } catch {
    return [];
  }
}

// ---- GraphQL queries ----
// NOTE: schema mengikuti pola v3 (mirip Uniswap v3):
// fields umum: id, timestamp, amountUSD, origin, sender, recipient, pool{token0{symbol address} token1{symbol address} feeTier}
const SWAPS_QUERY = gql`
  query SwapsByOrigin($addresses: [Bytes!]!, $start: Int!, $end: Int!, $first: Int!, $skip: Int!) {
    swaps(
      where: { origin_in: $addresses, timestamp_gte: $start, timestamp_lte: $end }
      orderBy: timestamp
      orderDirection: asc
      first: $first
      skip: $skip
    ) {
      id
      timestamp
      amountUSD
      origin
      sender
      recipient
      pool { feeTier token0 { symbol id: address } token1 { symbol id: address } }
    }
  }
`;
const SWAPS_QUERY_SENDER = gql`
  query SwapsBySender($addresses: [Bytes!]!, $start: Int!, $end: Int!, $first: Int!, $skip: Int!) {
    swaps(
      where: { sender_in: $addresses, timestamp_gte: $start, timestamp_lte: $end }
      orderBy: timestamp
      orderDirection: asc
      first: $first
      skip: $skip
    ) {
      id
      timestamp
      amountUSD
      origin
      sender
      recipient
      pool { feeTier token0 { symbol id: address } token1 { symbol id: address } }
    }
  }
`;
const SWAPS_QUERY_RECIPIENT = gql`
  query SwapsByRecipient($addresses: [Bytes!]!, $start: Int!, $end: Int!, $first: Int!, $skip: Int!) {
    swaps(
      where: { recipient_in: $addresses, timestamp_gte: $start, timestamp_lte: $end }
      orderBy: timestamp
      orderDirection: asc
      first: $first
      skip: $skip
    ) {
      id
      timestamp
      amountUSD
      origin
      sender
      recipient
      pool { feeTier token0 { symbol id: address } token1 { symbol id: address } }
    }
  }
`;

// ---- Core fetch with pagination & fallback ----
async function fetchAll(client, query, varsBase) {
  const first = 1000;
  let skip = 0;
  const out = [];
  while (true) {
    const vars = { ...varsBase, first, skip };
    const res = await client.request(query, vars);
    const rows = res?.swaps || [];
    out.push(...rows);
    if (rows.length < first) break;
    skip += first;
  }
  return out;
}

async function fetchWithFallbacks(addresses, start, end) {
  let lastErr;
  for (const url of DEFAULT_URLS) {
    const client = new GraphQLClient(url, { timeout: 30000 });
    try {
      // 1) by origin (primary for "execution volume")
      const swapsOrigin = await fetchAll(client, SWAPS_QUERY, { addresses, start, end });
      // 2) by sender/recipient (for involved volume & completeness)
      const swapsSender = await fetchAll(client, SWAPS_QUERY_SENDER, { addresses, start, end });
      const swapsRecipient = await fetchAll(client, SWAPS_QUERY_RECIPIENT, { addresses, start, end });
      return { swapsOrigin, swapsSender, swapsRecipient, endpoint: url };
    } catch (e) {
      lastErr = e;
      // try next endpoint
    }
  }
  throw new Error(`All subgraph endpoints failed. Last error: ${lastErr?.message || lastErr}`);
}

// ---- Aggregate ----
function aggregate(addresses, swapsOrigin, swapsSender, swapsRecipient) {
  const idxById = new Map();
  function addSet(rows, tag) {
    for (const s of rows) {
      if (!idxById.has(s.id)) idxById.set(s.id, { ...s, _tags: new Set([tag]) });
      else idxById.get(s.id)._tags.add(tag);
    }
  }
  addSet(swapsOrigin, 'origin');
  addSet(swapsSender, 'sender');
  addSet(swapsRecipient, 'recipient');

  const all = [...idxById.values()];

  // per address metrics
  const per = {};
  for (const a of addresses) {
    per[a] = {
      address: a,
      swaps_execution: 0,
      volume_execution_usd: 0,
      swaps_involved: 0,
      volume_involved_usd: 0,
      top_pairs: new Map(), // "TOKEN0/TOKEN1" -> usd
    };
  }

  for (const s of all) {
    const usd = Number(s.amountUSD || 0) || 0;
    const pair = `${s.pool?.token0?.symbol || 'T0'}/${s.pool?.token1?.symbol || 'T1'}`;

    // execution volume: attribute ONLY to origin (if it’s in our list)
    const o = (s.origin || '').toLowerCase();
    if (per[o]) {
      per[o].swaps_execution += 1;
      per[o].volume_execution_usd += usd;
      per[o].top_pairs.set(pair, (per[o].top_pairs.get(pair) || 0) + usd);
    }

    // involved volume: if any of origin/sender/recipient matches
    const involved = new Set(
      [s.origin, s.sender, s.recipient].map(x => (x || '').toLowerCase())
    );
    for (const addr of involved) {
      if (per[addr]) {
        per[addr].swaps_involved += 1;
        per[addr].volume_involved_usd += usd;
      }
    }
  }

  // finalize top pairs
  for (const a of Object.values(per)) {
    a.top_pairs = [...a.top_pairs.entries()]
      .sort((x, y) => y[1] - x[1])
      .slice(0, 5)
      .map(([pair, usd]) => `${pair} (${humanUSD(usd)})`);
  }

  return { per, allCount: all.length };
}

// ---- CSV writer ----
function writeCSV(filename, rows) {
  return new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(filename);
    const stream = csv.format({ headers: true });
    stream.pipe(ws);
    rows.forEach(r => stream.write(r));
    stream.end();
    ws.on('finish', resolve);
    ws.on('error', reject);
  });
}

// ---- Main ----
(async () => {
  const date = getArg('date') || new Date().toISOString().slice(0, 10); // YYYY-MM-DD UTC
  const min = Number(getArg('min') || '10000');
  const cliAddresses = parseAddressList(getArg('addresses'));
  const fileAddresses = loadWalletsFromFile(WALLET_FILE);
  const addresses = uniqLower([...cliAddresses, ...fileAddresses]);

  if (addresses.length === 0) {
    console.error('Tidak ada address. Isi account.txt atau pakai --addresses=0xabc,0xdef');
    process.exit(1);
  }

  const { start, end } = utcDayBounds(date);

  console.log('=== PancakeSwap v3 Volume Checker (BSC) ===');
  console.log(`Tanggal (UTC): ${date} | Rentang: ${start}..${end}`);
  console.log(`Wallets: ${addresses.length} | Threshold: ${humanUSD(min)}\n`);

  try {
    const { swapsOrigin, swapsSender, swapsRecipient, endpoint } = await fetchWithFallbacks(addresses, start, end);
    console.log(`Subgraph endpoint: ${endpoint}\n`);

    const { per, allCount } = aggregate(addresses, swapsOrigin, swapsSender, swapsRecipient);

    // Console table
    const table = addresses.map(a => {
      const x = per[a];
      return {
        wallet: a,
        swaps_exec: x.swaps_execution,
        vol_exec_usd: x.volume_execution_usd,
        swaps_involved: x.swaps_involved,
        vol_involved_usd: x.volume_involved_usd,
        pass: x.volume_execution_usd >= min ? '✅' : '❌',
        top_pairs: x.top_pairs.join('; ')
      };
    });

    // Pretty print
    console.table(table.map(r => ({
      wallet: r.wallet,
      swaps_exec: r.swaps_exec,
      vol_exec: humanUSD(r.vol_exec_usd),
      pass: r.pass,
      top_pairs: r.top_pairs
    })));

    // Write CSV
    const csvRows = table.map(r => ({
      wallet: r.wallet,
      swaps_execution: r.swaps_exec,
      volume_execution_usd: r.vol_exec_usd.toFixed(2),
      swaps_involved: r.swaps_involved,
      volume_involved_usd: r.vol_involved_usd.toFixed(2),
      pass: r.pass,
      top_pairs: r.top_pairs
    }));

    const outName = `volume-${date}.csv`;
    await writeCSV(outName, csvRows);
    console.log(`\nSaved: ${outName}`);
    console.log(`Total swaps (deduped across queries): ${allCount}`);
    console.log('\nCatatan: gunakan kolom "volume_execution_usd" untuk cek syarat harian per wallet (by origin).');
  } catch (e) {
    console.error('Gagal fetch/aggregate:', e.message);
    process.exit(1);
  }
})();
