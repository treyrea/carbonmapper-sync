import fetch from 'node-fetch';
import nodemailer from 'nodemailer';

// Supabase config
const SUPABASE_URL = 'https://loarybepuwfclbxaovro.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxvYXJ5YmVwdXdmY2xieGFvdnJvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzQyODg5OCwiZXhwIjoyMDYzMDA0ODk4fQ.xatv3SN4kXbwT8EU6Hf6XmadOpoDHh0LhsYwLXaOEsE';
const TABLE_NAME = 'plumes';
const PRIMARY_KEY = 'id';

// CarbonMapper config
const API_URL = 'https://api.carbonmapper.org/api/v1/catalog/plumes/annotated?limit=1000';

async function fetchCarbonMapperData() {
  const res = await fetch(API_URL);
  const json = await res.json();
  const now = new Date();
  const cutoff = new Date();
  cutoff.setDate(now.getDate() - 90);

  return json.features
    .map(f => ({ id: f.id, ...f.properties }))
    .filter(p => {
      const date = new Date(p.datetime);
      return date >= cutoff;
    });
}

async function upsertToSupabase(records) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${TABLE_NAME}?on_conflict=${PRIMARY_KEY}`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'resolution=merge-duplicates'
    },
    body: JSON.stringify(records)
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase insert error: ${err}`);
  }

  const data = await res.json();
  return data;
}

async function sendErrorEmail(message) {
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: 'treyrea@gmail.com',
      pass: process.env.GMAIL_PASS // <- You’ll set this as an env var on Render
    }
  });

  await transporter.sendMail({
    from: '"CarbonMapper Alerts" <treyrea@gmail.com>',
    to: 'treyrea@gmail.com',
    subject: 'CarbonMapper Sync Error',
    text: message
  });
}

(async () => {
  try {
    const plumes = await fetchCarbonMapperData();
    if (!plumes.length) {
      console.log('No new plumes found.');
      return;
    }

    const inserted = await upsertToSupabase(plumes);
    console.log(`Successfully upserted ${inserted.length} records.`);
  } catch (err) {
    console.error('Error occurred:', err.message);
    await sendErrorEmail(`Error occurred during sync:\n\n${err.message}`);
  }
})();
