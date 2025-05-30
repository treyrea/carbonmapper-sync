import axios from "axios";
import nodemailer from "nodemailer";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = "https://loarybepuwfclbxaovro.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxvYXJ5YmVwdXdmY2xieGFvdnJvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzQyODg5OCwiZXhwIjoyMDYzMDA0ODk4fQ.xatv3SN4kXbwT8EU6Hf6XmadOpoDHh0LhsYwLXaOEsE";
const TABLE_NAME = "plumes";
const API_URL = "https://api.carbonmapper.org/api/v1/catalog/plumes/annotated";
const RECIPIENT_EMAIL = "treyrea@gmail.com";
const APP_PASSWORD = "pwcbkpzduvygkeyu"; // NO SPACES

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
    console.log("📧 Error alert sent.");
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

    console.log("📦 Raw API response:", response.data);

    if (!response.data || !Array.isArray(response.data.items)) {
      throw new Error("CarbonMapper API returned unexpected data format");
    }

    return response.data.items;
  } catch (err) {
    await sendErrorEmail("API Fetch Failed", err.message);
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
    await sendErrorEmail("Supabase Upsert Failed", err.message);
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
    console.log("✅ Sync completed successfully.");
  } catch (err) {
    console.error("💥 Final script error:", err.message);
  }
})();
