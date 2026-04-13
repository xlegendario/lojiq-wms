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
  AIRTABLE_INCOMING_STOCK_TABLE = "Incoming Stock",
  AIRTABLE_SELLERS_TABLE = "Sellers Database",
  AIRTABLE_MERCHANTS_TABLE = "Merchants",
  AIRTABLE_INVENTORY_UNITS_TABLE = "Inventory Units",
  BUYERS_AIRTABLE_BASE_ID,
  BUYERS_AIRTABLE_TABLE = "Buyers Database",
  BUYERS_AIRTABLE_TOKEN
} = process.env;

if (!AIRTABLE_TOKEN) {
  throw new Error("Missing AIRTABLE_TOKEN environment variable");
}

if (!AIRTABLE_BASE_ID) {
  throw new Error("Missing AIRTABLE_BASE_ID environment variable");
}

const airtable = new Airtable({ apiKey: AIRTABLE_TOKEN }).base(AIRTABLE_BASE_ID);

if (!BUYERS_AIRTABLE_BASE_ID) {
  throw new Error("Missing BUYERS_AIRTABLE_BASE_ID environment variable");
}

const buyersBase = new Airtable({
  apiKey: BUYERS_AIRTABLE_TOKEN || AIRTABLE_TOKEN
}).base(BUYERS_AIRTABLE_BASE_ID);

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

async function findIncomingStockByGTIN(gtin) {
  const safeCode = escapeFormulaValue(gtin);

  const records = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
    .select({
      filterByFormula: `AND(
        TRIM({Product GTIN} & '') = '${safeCode}',
        TRIM({SKU} & '') != '',
        TRIM({Size} & '') != ''
      )`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function getInboundPartyOptions() {
  const sellerRecords = await airtable(AIRTABLE_SELLERS_TABLE)
    .select({
      fields: ["Full Name", "Supplier/Forwarder?"],
      filterByFormula: `{Supplier/Forwarder?} = 1`,
      sort: [{ field: "Full Name", direction: "asc" }]
    })
    .all();

  const merchantRecords = await airtable(AIRTABLE_MERCHANTS_TABLE)
    .select({
      fields: ["Store Name", "Supplier/Forwarder?"],
      filterByFormula: `{Supplier/Forwarder?} = 1`,
      sort: [{ field: "Store Name", direction: "asc" }]
    })
    .all();

  const sellerOptions = sellerRecords
    .map((record) => ({
      id: record.id,
      label: asText(record.fields["Full Name"]),
      source: "seller"
    }))
    .filter((option) => option.label);

  const merchantOptions = merchantRecords
    .map((record) => ({
      id: record.id,
      label: asText(record.fields["Store Name"]),
      source: "merchant"
    }))
    .filter((option) => option.label);

  return [...sellerOptions, ...merchantOptions];
}

async function getBuyerOptions() {
  const records = await buyersBase(BUYERS_AIRTABLE_TABLE)
    .select({
      fields: [
        "Full Name",
        "Company Name",
        "VAT ID",
        "Email",
        "Address",
        "Address line 2",
        "Zipcode",
        "City",
        "Country"
      ],
      sort: [{ field: "Full Name", direction: "asc" }]
    })
    .all();

  return records
    .map((record) => ({
      id: record.id,
      label: asText(record.fields["Full Name"]),
      details: {
        full_name: asText(record.fields["Full Name"]),
        company_name: asText(record.fields["Company Name"]),
        vat_id: asText(record.fields["VAT ID"]),
        email: asText(record.fields["Email"]),
        address: asText(record.fields["Address"]),
        address_line_2: asText(record.fields["Address line 2"]),
        zipcode: asText(record.fields["Zipcode"]),
        city: asText(record.fields["City"]),
        country: asText(record.fields["Country"])
      }
    }))
    .filter((option) => option.label);
}
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/api/inbound-parties", async (_req, res) => {
  try {
    const options = await getInboundPartyOptions();

    return res.status(200).json({
      ok: true,
      options
    });
  } catch (error) {
    console.error("inbound-parties failed:", error);
    return res.status(500).json({
      error: "Failed to load inbound party options",
      details: error.message
    });
  }
});

app.post("/api/lookup-product", async (req, res) => {
  try {
    const gtin = asText(req.body?.gtin);

    if (!gtin) {
      return res.status(400).json({ error: "Missing gtin" });
    }

    // 1. First search Incoming Stock
    let record = await findIncomingStockByGTIN(gtin);
    let source = "incoming_stock";

    // 2. Fallback to Stock Levels
    if (!record) {
      record = await findStockLevelByGTIN(gtin);
      source = "stock_levels";
    }

    if (!record) {
      return res.status(200).json({
        found: false,
        gtin,
        sku: "",
        size: "",
        source: null
      });
    }

    const fields = record.fields || {};

    return res.status(200).json({
      found: true,
      gtin,
      sku: asText(fields["SKU"]),
      size: asText(fields["Size"]),
      source
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
    const submittedType = asText(req.body?.type);
    const selectedPartyId = asText(req.body?.party_id);
    const selectedPartySource = asText(req.body?.party_source);
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    const typeToSave =
      submittedType === "Consignment" ||
      submittedType === "Forwarding" ||
      submittedType === "Regular"
        ? submittedType
        : null;
    const clientFieldValue =
      selectedPartySource === "merchant" && selectedPartyId
        ? [selectedPartyId]
        : [];
    
    const supplierFieldValue =
      selectedPartySource === "seller" && selectedPartyId
        ? [selectedPartyId]
        : [];

    if (!trackingNumber) {
      return res.status(400).json({ error: "Missing tracking_number" });
    }

    if (!items.length) {
      return res.status(400).json({ error: "No items provided" });
    }

    const now = new Date().toISOString();
    let createdCount = 0;
    let updatedCount = 0;

    const safeTracking = escapeFormulaValue(trackingNumber);

    const placeholderRecords = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
      .select({
        filterByFormula: `AND(
          TRIM({Tracking Number} & '') = '${safeTracking}',
          OR(
            {Product GTIN} = BLANK(),
            TRIM({Product GTIN} & '') = ''
          )
        )`,
        maxRecords: 1
      })
      .firstPage();

    let placeholderRecord = placeholderRecords[0] || null;

    const parcelReceivedAt = placeholderRecord?.fields?.["Received At"] || now;

    for (const item of items) {
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

      const safeGtin = escapeFormulaValue(gtin);

      const existingRecords = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
        .select({
          filterByFormula: `AND(
            TRIM({Tracking Number} & '') = '${safeTracking}',
            TRIM({Product GTIN} & '') = '${safeGtin}'
          )`,
          maxRecords: 1
        })
        .firstPage();

      const existingRecord = existingRecords[0] || null;

      const receivedAtToUse =
        placeholderRecord?.fields?.["Received At"] ||
        existingRecord?.fields?.["Received At"] ||
        now;

      if (existingRecord) {
        await airtable(AIRTABLE_INCOMING_STOCK_TABLE).update(existingRecord.id, {
          "Tracking Number": trackingNumber,
          "Product GTIN": gtin,
          "SKU": sku,
          "Size": size,
          "Quantity": quantity,
          "Type": typeToSave,
          "Client": clientFieldValue,
          "Supplier": supplierFieldValue,
          "Status": "Verified",
          "Verified At": now,
          "Received At": receivedAtToUse
        });

        updatedCount += 1;
        continue;
      }

      if (placeholderRecord) {
        await airtable(AIRTABLE_INCOMING_STOCK_TABLE).update(placeholderRecord.id, {
          "Tracking Number": trackingNumber,
          "Product GTIN": gtin,
          "SKU": sku,
          "Size": size,
          "Quantity": quantity,
          "Type": typeToSave,
          "Client": clientFieldValue,
          "Supplier": supplierFieldValue,
          "Status": "Verified",
          "Verified At": now,
          "Received At": receivedAtToUse
        });

        updatedCount += 1;
        placeholderRecord = null;
        continue;
      }

      await airtable(AIRTABLE_INCOMING_STOCK_TABLE).create({
        "Tracking Number": trackingNumber,
        "Product GTIN": gtin,
        "SKU": sku,
        "Size": size,
        "Quantity": quantity,
        "Type": typeToSave,
        "Client": clientFieldValue,
        "Supplier": supplierFieldValue,
        "Status": "Verified",
        "Verified At": now,
        "Received At": parcelReceivedAt
      });

      createdCount += 1;
    }

    return res.status(200).json({
      ok: true,
      created_count: createdCount,
      updated_count: updatedCount
    });
  } catch (error) {
    console.error("submit-inbound failed:", error);
    return res.status(500).json({
      error: "Failed to submit inbound parcel",
      details: error.message
    });
  }
});

app.post("/api/receive-parcel", async (req, res) => {
  try {
    const trackingNumber = asText(req.body?.tracking_number);

    if (!trackingNumber) {
      return res.status(400).json({ error: "Missing tracking_number" });
    }

    const now = new Date().toISOString();

    const records = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
      .select({
        filterByFormula: `TRIM({Tracking Number} & '') = '${escapeFormulaValue(trackingNumber)}'`,
        maxRecords: 1
      })
      .firstPage();

    if (records.length > 0) {
      await airtable(AIRTABLE_INCOMING_STOCK_TABLE).update(records[0].id, {
        "Status": "Received",
        "Received At": now
      });

      return res.json({ message: "Parcel updated", exists: true });
    }

    await airtable(AIRTABLE_INCOMING_STOCK_TABLE).create({
      "Tracking Number": trackingNumber,
      "Status": "Received",
      "Received At": now
    });

    res.json({ message: "Parcel created", exists: false });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Failed to process parcel",
      details: error.message
    });
  }
});

app.get("/api/outbound-buyers", async (_req, res) => {
  try {
    const options = await getBuyerOptions();

    return res.status(200).json({
      ok: true,
      options
    });
  } catch (error) {
    console.error("outbound-buyers failed:", error);
    return res.status(500).json({
      error: "Failed to load buyers",
      details: error.message
    });
  }
});

app.post("/api/outbound-buyers", async (req, res) => {
  try {
    const fullName = asText(req.body?.full_name);
    const companyName = asText(req.body?.company_name);
    const vatId = asText(req.body?.vat_id);
    const email = asText(req.body?.email);
    const address = asText(req.body?.address);
    const addressLine2 = asText(req.body?.address_line_2);
    const zipcode = asText(req.body?.zipcode);
    const city = asText(req.body?.city);
    const country = asText(req.body?.country);

    if (!fullName || !email || !address || !zipcode || !city || !country) {
      return res.status(400).json({
        error: "Missing required buyer fields"
      });
    }

    const created = await buyersBase(BUYERS_AIRTABLE_TABLE).create({
      "Full Name": fullName,
      "Company Name": companyName || null,
      "VAT ID": vatId || null,
      "Email": email,
      "Address": address,
      "Address line 2": addressLine2 || null,
      "Zipcode": zipcode,
      "City": city,
      "Country": country
    });

    return res.status(200).json({
      ok: true,
      option: {
        id: created.id,
        label: asText(created.fields["Full Name"]),
        details: {
          full_name: asText(created.fields["Full Name"]),
          company_name: asText(created.fields["Company Name"]),
          vat_id: asText(created.fields["VAT ID"]),
          email: asText(created.fields["Email"]),
          address: asText(created.fields["Address"]),
          address_line_2: asText(created.fields["Address line 2"]),
          zipcode: asText(created.fields["Zipcode"]),
          city: asText(created.fields["City"]),
          country: asText(created.fields["Country"])
        }
      }
    });
  } catch (error) {
    console.error("create outbound buyer failed:", error);
    return res.status(500).json({
      error: "Failed to create buyer",
      details: error.message
    });
  }
});

app.post("/api/outbound-lookup-gtin", async (req, res) => {
  try {
    const gtin = asText(req.body?.gtin);

    if (!gtin) {
      return res.status(400).json({ error: "Missing gtin" });
    }

    const safeGtin = escapeFormulaValue(gtin);

    const records = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
      .select({
        filterByFormula: `AND(
          TRIM({Product GTIN} & '') = '${safeGtin}',
          {Availability Status} = 'Available'
        )`
      })
      .all();

    if (!records.length) {
      const anyMatch = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
        .select({
          filterByFormula: `TRIM({Product GTIN} & '') = '${safeGtin}'`,
          maxRecords: 1
        })
        .firstPage();

      if (anyMatch.length > 0) {
        return res.status(200).json({
          found: false,
          reason: "no_available_items"
        });
      }

      return res.status(200).json({
        found: false,
        reason: "unknown_gtin"
      });
    }

    const first = records[0];
    const productName = asText(first.fields["Product Name"]);
    const sku = asText(first.fields["SKU"]);
    const size = asText(first.fields["Size"]);

    const purchasePrices = records
      .map((r) => Number(r.fields["Purchase Price"]))
      .filter((n) => Number.isFinite(n));

    const totalPrice = purchasePrices.reduce((sum, n) => sum + n, 0);
    const averagePrice = purchasePrices.length ? totalPrice / purchasePrices.length : 0;

    return res.status(200).json({
      found: true,
      gtin,
      product_name: productName,
      sku,
      size,
      available_quantity: records.length,
      unit_price: averagePrice,
      total_available_price: totalPrice
    });
  } catch (error) {
    console.error("outbound-lookup-gtin failed:", error);
    return res.status(500).json({
      error: "Failed to lookup outbound GTIN",
      details: error.message
    });
  }
});

app.post("/api/outbound-search-sku-size", async (req, res) => {
  try {
    const sku = asText(req.body?.sku).toUpperCase();
    const size = asText(req.body?.size);

    if (!sku || !size) {
      return res.status(400).json({ error: "Missing sku or size" });
    }

    const safeSku = escapeFormulaValue(sku);
    const safeSize = escapeFormulaValue(size);

    const records = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
      .select({
        filterByFormula: `AND(
          UPPER(TRIM({SKU} & '')) = '${safeSku}',
          TRIM({Size} & '') = '${safeSize}',
          {Availability Status} = 'Available'
        )`
      })
      .all();

    if (!records.length) {
      return res.status(200).json({
        found: false,
        reason: "not_found"
      });
    }

    const first = records[0];
    const gtin = asText(first.fields["Product GTIN"]);
    const productName = asText(first.fields["Product Name"]);

    const purchasePrices = records
      .map((r) => Number(r.fields["Purchase Price"]))
      .filter((n) => Number.isFinite(n));

    const totalPrice = purchasePrices.reduce((sum, n) => sum + n, 0);
    const averagePrice = purchasePrices.length ? totalPrice / purchasePrices.length : 0;

    return res.status(200).json({
      found: true,
      gtin,
      product_name: productName,
      sku,
      size,
      available_quantity: records.length,
      unit_price: averagePrice,
      total_available_price: totalPrice
    });
  } catch (error) {
    console.error("outbound-search-sku-size failed:", error);
    return res.status(500).json({
      error: "Failed to search outbound SKU/Size",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq WMS running on port ${PORT}`);
});
