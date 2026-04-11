import dotenv from "dotenv";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

dotenv.config();

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "2mb" }));

const {
  PORT = 3000,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_STOCK_LEVELS_TABLE = "Stock Levels",
  AIRTABLE_INCOMING_STOCK_TABLE = "Incoming Stock"
} = process.env;

if (!AIRTABLE_TOKEN) {
  throw new Error("Missing AIRTABLE_TOKEN environment variable");
}

if (!AIRTABLE_BASE_ID) {
  throw new Error("Missing AIRTABLE_BASE_ID environment variable");
}

const airtable = new Airtable({ apiKey: AIRTABLE_TOKEN }).base(AIRTABLE_BASE_ID);

function asText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function escapeFormulaValue(value) {
  return asText(value).replace(/'/g, "\\'");
}

async function findStockLevelByGTIN(gtin) {
  const safeCode = escapeFormulaValue(gtin);

  const records = await airtable(AIRTABLE_STOCK_LEVELS_TABLE)
    .select({
      filterByFormula: `TRIM({Product GTIN} & '') = '${safeCode}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.post("/api/lookup-product", async (req, res) => {
  try {
    const gtin = asText(req.body?.gtin);

    if (!gtin) {
      return res.status(400).json({ error: "Missing gtin" });
    }

    const record = await findStockLevelByGTIN(gtin);

    if (!record) {
      return res.status(200).json({
        found: false,
        gtin,
        sku: "",
        size: ""
      });
    }

    const fields = record.fields || {};

    return res.status(200).json({
      found: true,
      gtin,
      sku: asText(fields["SKU"]),
      size: asText(fields["Size"])
    });
  } catch (error) {
    console.error("lookup-product failed:", error);
    return res.status(500).json({
      error: "Failed to lookup product",
      details: error.message
    });
  }
});

app.post("/api/submit-inbound", async (req, res) => {
  try {
    const trackingNumber = asText(req.body?.tracking_number);
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    if (!trackingNumber) {
      return res.status(400).json({ error: "Missing tracking_number" });
    }

    if (!items.length) {
      return res.status(400).json({ error: "No items provided" });
    }

    const recordsToCreate = items.map((item) => {
      const gtin = asText(item.gtin);
      const sku = asText(item.sku);
      const size = asText(item.size);
      const quantity = Number(item.quantity) || 0;

      if (!gtin) {
        throw new Error("One or more items are missing Product GTIN");
      }

      if (quantity <= 0) {
        throw new Error(`Invalid quantity for GTIN ${gtin}`);
      }

      return {
        fields: {
          "Tracking Number": trackingNumber,
          "Product GTIN": gtin,
          "SKU": sku,
          "Size": size,
          "Quantity": quantity,
          "Status": "Verified",
          "Verified At": new Date().toISOString()
        }
      };
    });

    const createdRecords = [];

    for (let i = 0; i < recordsToCreate.length; i += 10) {
      const batch = recordsToCreate.slice(i, i + 10);
      const createdBatch = await airtable(AIRTABLE_INCOMING_STOCK_TABLE).create(batch);
      createdRecords.push(...createdBatch);
    }

    return res.status(200).json({
      ok: true,
      created_count: createdRecords.length
    });
  } catch (error) {
    console.error("submit-inbound failed:", error);
    return res.status(500).json({
      error: "Failed to submit inbound parcel",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq WMS running on port ${PORT}`);
});
