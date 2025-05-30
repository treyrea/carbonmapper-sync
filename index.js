const axios = require("axios");
const nodemailer = require("nodemailer");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = "https://loarybepuwfclbxaovro.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxvYXJ5YmVwdXdmY2xieGFvdnJvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzQyODg5OCwiZXhwIjoyMDYzMDA0ODk4fQ.xatv3SN4kXbwT8EU6Hf6XmadOpoDHh0LhsYwLXaOEsE";
const TABLE_NAME = "plumes";
const API_URL = "https://api.carbonmapper.org/plumes";
const RECIPIENT_EMAIL = "treyrea@gmail.com";

// Replace this with your Gmail app password — no spaces!
const APP_PASSWORD = "pwcbkpzduvygkeyu";

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: RECIPIENT_EMAIL,
    pass: APP_PASSWORD
  }
});

async function sendErrorEmail(subject, message) {
  try {
    await transporter.sendMail({
      from: RECIPIENT_EMAIL,
      to: RECIPIENT_EMAIL,
      subject,
      text: message
    });
    console.log("🚨 Error email sent.");
  } catch (err) {
    console.error("❌ Failed to send error email:", err.message);
  }
}

async function fetchCarbonMapperData() {
  try {
    const today = new Date();
    const startDate = new Date(today.setDate(today.getDate() - 90)).toISOString();

    const response = await axios.get(API_URL, {
      params: {
        limit: 1000,
        start_date: startDate
      }
    });

    if (!response.data || !Array.isArray(response.data.items)) {
      throw new Error("Invalid or missing data format in CarbonMapper API response");
    }

    return response.data.items;
  } catch (err) {
    await sendErrorEmail("CarbonMapper Sync Error - API Fetch Failed", err.message);
    throw err;
  }
}

async function upsertToSupabase(data) {
  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  try {
    const { error } = await supabase.from(TABLE_NAME).upsert(data, {
      onConflict: ["id"]
    });

    if (error) throw error;
  } catch (err) {
    await sendErrorEmail("CarbonMapper Sync Error - Supabase Upsert Failed", err.message);
    throw err;
  }
}

(async () => {
  try {
    const items = await fetchCarbonMapperData();
    const flattened = items.map(item => ({
      id: item.id,
      plume_id: item.plume_id,
      gas: item.gas,
      scene_id: item.scene_id,
      scene_timestamp: item.scene_timestamp,
      instrument: item.instrument,
      mission_phase: item.mission_phase,
      platform: item.platform,
      emission_auto: item.emission_auto,
      longitude: item.geometry_json?.coordinates?.[0] ?? null,
      latitude: item.geometry_json?.coordinates?.[1] ?? null
    }));

    await upsertToSupabase(flattened);
    console.log("✅ Sync successful");
  } catch (err) {
    console.error("❌ Final error:", err.message);
  }
})();
