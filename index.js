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
  AIRTABLE_EXTERNAL_SALES_LOG_TABLE = "External Sales Log",
  AIRTABLE_FORWARDING_SERVICE_LOG_TABLE = "Forwarding Service Log",
  AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE = "Unfulfilled Orders Log",
  BUYERS_AIRTABLE_BASE_ID,
  AIRTABLE_BUYERS_TABLE = "Buyers Database", // Main Airtable
  BUYERS_AIRTABLE_TABLE = "Buyer Database",  // External Airtable
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

async function updateInventoryUnitsToForwardPending(recordIds) {
  const uniqueIds = [...new Set((recordIds || []).filter(Boolean))];

  for (let i = 0; i < uniqueIds.length; i += 10) {
    const batch = uniqueIds.slice(i, i + 10);

    await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).update(
      batch.map((id) => ({
        id,
        fields: {
          "Availability Status": "Forward Pending"
        }
      }))
    );
  }
}

async function updateInventoryUnitsToReserved(recordIds) {
  const uniqueIds = [...new Set((recordIds || []).filter(Boolean))];

  for (let i = 0; i < uniqueIds.length; i += 10) {
    const batch = uniqueIds.slice(i, i + 10);

    await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).update(
      batch.map((id) => ({
        id,
        fields: {
          "Availability Status": "Reserved",
          "Selling Method": "Kickz Caviar"
        }
      }))
    );
  }
}

async function getAverageForwardingFeeForSellerIds(sellerIds) {
  const uniqueSellerIds = [...new Set((sellerIds || []).filter(Boolean))];
  if (!uniqueSellerIds.length) return 0;

  const sellerRecords = await Promise.all(
    uniqueSellerIds.map((id) => airtable(AIRTABLE_SELLERS_TABLE).find(id))
  );

  const fees = sellerRecords
    .map((record) => Number(record.fields["Forwarding Fee"]))
    .filter((value) => Number.isFinite(value));

  if (!fees.length) return 0;
  return fees.reduce((sum, value) => sum + value, 0) / fees.length;
}

async function updateInventoryUnitsToSold(recordIds) {
  const uniqueIds = [...new Set((recordIds || []).filter(Boolean))];

  for (let i = 0; i < uniqueIds.length; i += 10) {
    const batch = uniqueIds.slice(i, i + 10);

    await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).update(
      batch.map((id) => ({
        id,
        fields: {
          "Availability Status": "Sold"
        }
      }))
    );
  }
}

async function findMainBuyerRecordByBuyerId(buyerIdValue) {
  const safeBuyerId = escapeFormulaValue(buyerIdValue);

  const records = await airtable(AIRTABLE_BUYERS_TABLE)
    .select({
      fields: ["Buyer ID"],
      filterByFormula: `TRIM({Buyer ID} & '') = '${safeBuyerId}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
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

function parseTrackingNumbers(value) {
  return asText(value)
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function isWarehouseItemId(itemId) {
  const value = asText(itemId).toUpperCase();
  return value.startsWith("PCS-") || value.startsWith("KC-") || value.startsWith("RSC-");
}

async function getPackShipOutboundOptions() {
  const salesRecords = await airtable(AIRTABLE_EXTERNAL_SALES_LOG_TABLE)
    .select({
      fields: [
        "External Deal ID",
        "Buyer Name",
        "Shipping Status",
        "Tracking Numbers"
      ],
      filterByFormula: `{Shipping Status} = 'Ready to Ship'`
    })
    .all();

  const forwardingRecords = await airtable(AIRTABLE_FORWARDING_SERVICE_LOG_TABLE)
    .select({
      fields: [
        "Forwarding ID",
        "Buyer Name",
        "Shipping Status",
        "Tracking Numbers"
      ],
      filterByFormula: `{Shipping Status} = 'Ready to Ship'`
    })
    .all();

  const unfulfilledRecords = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
    .select({
      fields: [
        "Shopify Order Number",
        "Store Name",
        "Fulfillment Status",
        "Shipping Status",
        "Linked Inventory Unit",
        "Shipping Label"
      ],
      filterByFormula: `AND(
        {Fulfillment Status} = 'Ready to Ship',
        OR(
          {Shipping Status} = BLANK(),
          TRIM({Shipping Status} & '') = '',
          {Shipping Status} = 'Pending'
        )
      )`
    })
    .all();

  const salesOptions = salesRecords
    .map((record) => {
      const shippingStatus = asText(record.fields["Shipping Status"]);
      const trackingNumbers = parseTrackingNumbers(record.fields["Tracking Numbers"]);
      const externalDealId = asText(record.fields["External Deal ID"]);
      const buyerName = asText(record.fields["Buyer Name"]);

      return {
        id: record.id,
        source_table: "external_sales_log",
        label: `${externalDealId || record.id} - ${buyerName || "Unknown Buyer"}`,
        shipping_status: shippingStatus,
        tracking_numbers_count: trackingNumbers.length
      };
    })
    .filter((option) => option.tracking_numbers_count > 0);

  const forwardingOptions = forwardingRecords
    .map((record) => {
      const shippingStatus = asText(record.fields["Shipping Status"]);
      const trackingNumbers = parseTrackingNumbers(record.fields["Tracking Numbers"]);
      const forwardingId = asText(record.fields["Forwarding ID"]);
      const buyerName = asText(record.fields["Buyer Name"]);

      return {
        id: record.id,
        source_table: "forwarding_service_log",
        label: `${forwardingId || record.id} - ${buyerName || "Unknown Buyer"}`,
        shipping_status: shippingStatus,
        tracking_numbers_count: trackingNumbers.length
      };
    })
    .filter((option) => option.tracking_numbers_count > 0);

  const unfulfilledInventoryIds = [
    ...new Set(
      unfulfilledRecords.flatMap((record) =>
        Array.isArray(record.fields["Linked Inventory Unit"])
          ? record.fields["Linked Inventory Unit"]
          : []
      )
    )
  ];

  const inventoryUnitsById = new Map();
  for (const id of unfulfilledInventoryIds) {
    const record = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).find(id);
    inventoryUnitsById.set(id, record);
  }

  const groupedOrders = new Map();

  for (const record of unfulfilledRecords) {
    const shopifyOrderNumber = asText(record.fields["Shopify Order Number"]);
    const storeName = asText(record.fields["Store Name"]);
    const linkedInventoryUnitIds = Array.isArray(record.fields["Linked Inventory Unit"])
      ? record.fields["Linked Inventory Unit"]
      : [];

    const warehouseInventoryUnitIds = linkedInventoryUnitIds.filter((id) => {
      const inventoryRecord = inventoryUnitsById.get(id);
      return inventoryRecord && isWarehouseItemId(inventoryRecord.fields["Item ID"]);
    });

    if (!warehouseInventoryUnitIds.length) continue;

    const groupKey = `${shopifyOrderNumber}||${storeName}`;

    if (!groupedOrders.has(groupKey)) {
      groupedOrders.set(groupKey, {
        id: groupKey,
        source_table: "unfulfilled_orders_log",
        label: `${shopifyOrderNumber || record.id} - ${storeName || "Unknown Store"}`,
        shipping_status: asText(record.fields["Shipping Status"]) || "Pending",
        tracking_numbers_count: 1
      });
    }
  }

  return [...salesOptions, ...forwardingOptions, ...Array.from(groupedOrders.values())];
}

async function getForwardingSellerOptions() {
  const records = await airtable(AIRTABLE_SELLERS_TABLE)
    .select({
      fields: ["Full Name", "Supplier/Forwarder?"],
      filterByFormula: `{Supplier/Forwarder?} = 1`,
      sort: [{ field: "Full Name", direction: "asc" }]
    })
    .all();

  return records
    .map((record) => ({
      id: record.id,
      label: asText(record.fields["Full Name"])
    }))
    .filter((option) => option.label);
}

async function getSellerCodeByRecordId(sellerRecordId) {
  const record = await airtable(AIRTABLE_SELLERS_TABLE).find(sellerRecordId);
  return asText(record.fields["Seller ID"]);
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

async function getBuyerCountryOptions() {
  const token = BUYERS_AIRTABLE_TOKEN || AIRTABLE_TOKEN;

  const response = await fetch(`https://api.airtable.com/v0/meta/bases/${BUYERS_AIRTABLE_BASE_ID}/tables`, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data?.error?.message || "Failed to load buyer country options");
  }

  const table = (data.tables || []).find((entry) => entry.name === BUYERS_AIRTABLE_TABLE);
  if (!table) {
    throw new Error(`Table "${BUYERS_AIRTABLE_TABLE}" not found in buyers base`);
  }

  const countryField = (table.fields || []).find((field) => field.name === "Country");
  if (!countryField) {
    throw new Error('Field "Country" not found in buyers table');
  }

  const choices = countryField.options?.choices || [];

  return choices
    .map((choice) => asText(choice.name))
    .filter(Boolean);
}

async function getPackShipOutboundDetails(outboundId, sourceTable) {
  if (sourceTable === "unfulfilled_orders_log") {
    const [shopifyOrderNumber, storeName] = outboundId.split("||");

    const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
      .select({
        fields: [
          "Shopify Order Number",
          "Store Name",
          "Fulfillment Status",
          "Shipping Status",
          "Linked Inventory Unit",
          "Shipping Label"
        ],
        filterByFormula: `AND(
          TRIM({Shopify Order Number} & '') = '${escapeFormulaValue(shopifyOrderNumber)}',
          TRIM({Store Name} & '') = '${escapeFormulaValue(storeName)}',
          {Fulfillment Status} = 'Ready to Ship',
          OR(
            {Shipping Status} = BLANK(),
            TRIM({Shipping Status} & '') = '',
            {Shipping Status} = 'Pending'
          )
        )`
      })
      .all();

    const linkedInventoryUnitIds = [
      ...new Set(
        records.flatMap((record) =>
          Array.isArray(record.fields["Linked Inventory Unit"])
            ? record.fields["Linked Inventory Unit"]
            : []
        )
      )
    ];

    const inventoryUnitRecords = await Promise.all(
      linkedInventoryUnitIds.map((id) => airtable(AIRTABLE_INVENTORY_UNITS_TABLE).find(id))
    );

    const items = inventoryUnitRecords
      .filter((itemRecord) => isWarehouseItemId(itemRecord.fields["Item ID"]))
      .map((itemRecord) => ({
        id: itemRecord.id,
        gtin: asText(itemRecord.fields["Product GTIN"]),
        product_name: asText(itemRecord.fields["Product Name"]),
        sku: asText(itemRecord.fields["SKU"]),
        size: asText(itemRecord.fields["Size"])
      }));

    const firstLabelRecord = records.find((record) => {
      const files = Array.isArray(record.fields["Shipping Label"]) ? record.fields["Shipping Label"] : [];
      return files.length > 0;
    });

    return {
      id: outboundId,
      source_table: "unfulfilled_orders_log",
      shipping_status: "Ready To Ship",
      tracking_numbers: ["ORDER"],
      shipping_labels: firstLabelRecord && Array.isArray(firstLabelRecord.fields["Shipping Label"])
        ? firstLabelRecord.fields["Shipping Label"]
        : [],
      items
    };
  }

  const tableName =
    sourceTable === "forwarding_service_log"
      ? AIRTABLE_FORWARDING_SERVICE_LOG_TABLE
      : AIRTABLE_EXTERNAL_SALES_LOG_TABLE;

  const record = await airtable(tableName).find(outboundId);

  const trackingNumbers = parseTrackingNumbers(record.fields["Tracking Numbers"]);
  const linkedInventoryUnitIds = Array.isArray(record.fields["Linked Inventory Units"])
    ? record.fields["Linked Inventory Units"]
    : [];

  const inventoryUnitRecords = await Promise.all(
    linkedInventoryUnitIds.map((id) => airtable(AIRTABLE_INVENTORY_UNITS_TABLE).find(id))
  );

  const items = inventoryUnitRecords.map((itemRecord) => ({
    id: itemRecord.id,
    gtin: asText(itemRecord.fields["Product GTIN"]),
    product_name: asText(itemRecord.fields["Product Name"]),
    sku: asText(itemRecord.fields["SKU"]),
    size: asText(itemRecord.fields["Size"])
  }));

  return {
    id: record.id,
    source_table: sourceTable === "forwarding_service_log" ? "forwarding_service_log" : "external_sales_log",
    shipping_status: asText(record.fields["Shipping Status"]),
    tracking_numbers: trackingNumbers,
    shipping_labels: Array.isArray(record.fields["Shipping Labels"])
      ? record.fields["Shipping Labels"]
      : [],
    items
  };
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

      if (!gtin && (!sku || !size)) {
        throw new Error("One or more items are missing both Product GTIN and SKU/Size");
      }

      if (quantity <= 0) {
        throw new Error(`Invalid quantity for item ${gtin || `${sku} / ${size}`}`);
      }

      const safeGtin = escapeFormulaValue(gtin);
      const safeSku = escapeFormulaValue(sku);
      const safeSize = escapeFormulaValue(size);

      const existingRecords = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
        .select({
          filterByFormula: gtin
            ? `AND(
                TRIM({Tracking Number} & '') = '${safeTracking}',
                TRIM({Product GTIN} & '') = '${safeGtin}'
              )`
            : `AND(
                TRIM({Tracking Number} & '') = '${safeTracking}',
                OR(
                  {Product GTIN} = BLANK(),
                  TRIM({Product GTIN} & '') = ''
                ),
                TRIM({SKU} & '') = '${safeSku}',
                TRIM({Size} & '') = '${safeSize}'
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
          "Product GTIN": gtin || null,
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
          "Product GTIN": gtin || null,
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
        "Product GTIN": gtin || null,
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
    const safeTracking = escapeFormulaValue(trackingNumber);

    // 1. First check Incoming Stock (existing behavior)
    const incomingRecords = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
      .select({
        filterByFormula: `TRIM({Tracking Number} & '') = '${safeTracking}'`,
        maxRecords: 1
      })
      .firstPage();

    if (incomingRecords.length > 0) {
      await airtable(AIRTABLE_INCOMING_STOCK_TABLE).update(incomingRecords[0].id, {
        "Status": "Received",
        "Received At": now
      });

      return res.json({
        message: "Parcel updated",
        exists: true
      });
    }

    // 2. If not found in Incoming Stock, check Unfulfilled Orders Log
    const unfulfilledRecords = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
      .select({
        fields: [
          "Order ID",
          "Fulfillment Status",
          "StockX Tracking Number",
          "GOAT Tracking Number"
        ],
        filterByFormula: `OR(
          TRIM({StockX Tracking Number} & '') = '${safeTracking}',
          TRIM({GOAT Tracking Number} & '') = '${safeTracking}'
        )`
      })
      .all();

    if (unfulfilledRecords.length > 0) {
      const nonAwaitingLabelRecord = unfulfilledRecords.find((record) => {
        const fulfillmentStatus = asText(record.fields["Fulfillment Status"]);
        return fulfillmentStatus !== "Awaiting Label";
      });
    
      if (nonAwaitingLabelRecord) {
        const orderId = asText(nonAwaitingLabelRecord.fields["Order ID"]) || nonAwaitingLabelRecord.id;
    
        return res.status(400).json({
          error: `The Order ${orderId} might be cancelled or already shipped, check Airtable`
        });
      }
    
      for (let i = 0; i < unfulfilledRecords.length; i += 10) {
        const batch = unfulfilledRecords.slice(i, i + 10);
    
        await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(
          batch.map((record) => ({
            id: record.id,
            fields: {
              "Fulfillment Status": "Requested Label"
            }
          }))
        );
      }
    
      const firstOrderId =
        asText(unfulfilledRecords[0].fields["Order ID"]) || unfulfilledRecords[0].id;
    
      return res.json({
        message: `Label is requested for ${firstOrderId} successfully`,
        exists: false,
        matched_unfulfilled_order: true,
        order_id: firstOrderId
      });
    }

    // 3. Fallback: create placeholder in Incoming Stock (existing behavior)
    await airtable(AIRTABLE_INCOMING_STOCK_TABLE).create({
      "Tracking Number": trackingNumber,
      "Status": "Received",
      "Received At": now
    });

    return res.json({
      message: "Parcel created",
      exists: false
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      error: "Failed to process parcel",
      details: error.message
    });
  }
});

app.get("/api/pack-ship-outbounds", async (_req, res) => {
  try {
    const outbounds = await getPackShipOutboundOptions();

    return res.status(200).json({
      ok: true,
      outbounds
    });
  } catch (error) {
    console.error("pack-ship-outbounds failed:", error);
    return res.status(500).json({
      error: "Failed to load pack & ship outbounds",
      details: error.message
    });
  }
});

app.get("/api/pack-ship-outbound/:id", async (req, res) => {
  try {
    const outboundId = asText(req.params?.id);
    const sourceTable = asText(req.query?.source_table);

    if (!outboundId) {
      return res.status(400).json({ error: "Missing outbound id" });
    }

    if (!sourceTable) {
      return res.status(400).json({ error: "Missing source_table" });
    }

    const outbound = await getPackShipOutboundDetails(outboundId, sourceTable);

    return res.status(200).json({
      ok: true,
      outbound
    });
  } catch (error) {
    console.error("pack-ship-outbound-details failed:", error);
    return res.status(500).json({
      error: "Failed to load outbound details",
      details: error.message
    });
  }
});

app.post("/api/submit-pack-ship", async (req, res) => {
  try {
    const outboundId = asText(req.body?.outbound_id);
    const sourceTable = asText(req.body?.source_table);
    const itemsPerParcel = asText(req.body?.items_per_parcel);
    const packedInventoryUnitIds = Array.isArray(req.body?.packed_inventory_unit_ids)
      ? req.body.packed_inventory_unit_ids.map((id) => asText(id)).filter(Boolean)
      : [];

    if (!outboundId) {
      return res.status(400).json({ error: "Missing outbound_id" });
    }

    if (!sourceTable) {
      return res.status(400).json({ error: "Missing source_table" });
    }

    if (!itemsPerParcel) {
      return res.status(400).json({ error: "Missing items_per_parcel" });
    }

    if (!packedInventoryUnitIds.length) {
      return res.status(400).json({ error: "No packed inventory unit ids provided" });
    }

    if (sourceTable === "unfulfilled_orders_log") {
      const [shopifyOrderNumber, storeName] = outboundId.split("||");

      const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
        .select({
          fields: [
            "Shopify Order Number",
            "Store Name",
            "Fulfillment Status",
            "Shipping Status",
            "Linked Inventory Unit"
          ],
          filterByFormula: `AND(
            TRIM({Shopify Order Number} & '') = '${escapeFormulaValue(shopifyOrderNumber)}',
            TRIM({Store Name} & '') = '${escapeFormulaValue(storeName)}',
            {Fulfillment Status} = 'Ready to Ship',
            OR(
              {Shipping Status} = BLANK(),
              TRIM({Shipping Status} & '') = '',
              {Shipping Status} = 'Pending'
            )
          )`
        })
        .all();

      const matchingRecordIds = [];

      for (const record of records) {
        const linkedIds = Array.isArray(record.fields["Linked Inventory Unit"])
          ? record.fields["Linked Inventory Unit"]
          : [];

        const inventoryUnitRecords = await Promise.all(
          linkedIds.map((id) => airtable(AIRTABLE_INVENTORY_UNITS_TABLE).find(id))
        );

        const hasWarehouseItem = inventoryUnitRecords.some((itemRecord) =>
          isWarehouseItemId(itemRecord.fields["Item ID"])
        );

        if (hasWarehouseItem) {
          matchingRecordIds.push(record.id);
        }
      }

      for (let i = 0; i < matchingRecordIds.length; i += 10) {
        const batch = matchingRecordIds.slice(i, i + 10);

        await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(
          batch.map((id) => ({
            id,
            fields: {
              "Shipping Status": "Shipped"
            }
          }))
        );
      }

      await updateInventoryUnitsToSold(packedInventoryUnitIds);

      return res.status(200).json({
        ok: true
      });
    }

    const tableName =
      sourceTable === "forwarding_service_log"
        ? AIRTABLE_FORWARDING_SERVICE_LOG_TABLE
        : AIRTABLE_EXTERNAL_SALES_LOG_TABLE;

    await airtable(tableName).update(outboundId, {
      "Items per Parcel": itemsPerParcel,
      "Shipping Status": "Shipped"
    });

    if (sourceTable === "forwarding_service_log") {
      const uniqueIds = [...new Set(packedInventoryUnitIds)];

      for (let i = 0; i < uniqueIds.length; i += 10) {
        const batch = uniqueIds.slice(i, i + 10);

        await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).update(
          batch.map((id) => ({
            id,
            fields: {
              "Availability Status": "Forwarded"
            }
          }))
        );
      }
    } else {
      await updateInventoryUnitsToSold(packedInventoryUnitIds);
    }

    return res.status(200).json({
      ok: true
    });
  } catch (error) {
    console.error("submit-pack-ship failed:", error);
    return res.status(500).json({
      error: "Failed to submit pack & ship",
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

app.get("/api/outbound-forwarding-sellers", async (_req, res) => {
  try {
    const options = await getForwardingSellerOptions();

    return res.status(200).json({
      ok: true,
      options
    });
  } catch (error) {
    console.error("outbound-forwarding-sellers failed:", error);
    return res.status(500).json({
      error: "Failed to load forwarding sellers",
      details: error.message
    });
  }
});

app.get("/api/outbound-buyer-country-options", async (_req, res) => {
  try {
    const options = await getBuyerCountryOptions();

    return res.status(200).json({
      ok: true,
      options
    });
  } catch (error) {
    console.error("outbound-buyer-country-options failed:", error);
    return res.status(500).json({
      error: "Failed to load buyer country options",
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

    // 1. Create buyer in external Airtable
    const createdExternal = await buyersBase(BUYERS_AIRTABLE_TABLE).create({
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
    
    // 2. Reload to obtain formula fields
    const externalRecords = await buyersBase(BUYERS_AIRTABLE_TABLE)
      .select({
        fields: [
          "Buyer ID",
          "Country Code",
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
        filterByFormula: `RECORD_ID() = '${createdExternal.id}'`,
        maxRecords: 1
      })
      .firstPage();
    
    const created = externalRecords[0];
    
    const buyerIdValue = asText(created.fields["Buyer ID"]);
    const countryCodeValue = asText(created.fields["Country Code"]);
    
    // 3. Create buyer in main Airtable if not exists
    const existingMainBuyer = await findMainBuyerRecordByBuyerId(buyerIdValue);
    
    if (!existingMainBuyer) {
      await airtable(AIRTABLE_BUYERS_TABLE).create({
        "Buyer ID": buyerIdValue,
        "Country Code": countryCodeValue || null,
        "Full Name": asText(created.fields["Full Name"]),
        "Company Name": asText(created.fields["Company Name"]) || null,
        "VAT ID": asText(created.fields["VAT ID"]) || null,
        "Email": asText(created.fields["Email"]),
        "Address": asText(created.fields["Address"]),
        "Address line 2": asText(created.fields["Address line 2"]) || null,
        "Zipcode": asText(created.fields["Zipcode"]),
        "City": asText(created.fields["City"]),
        "Country": asText(created.fields["Country"])
      });
    }

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
    const mode = asText(req.body?.mode) || "Selling";
    const sellerId = asText(req.body?.seller_id);

    if (!gtin) {
      return res.status(400).json({ error: "Missing gtin" });
    }

    const safeGtin = escapeFormulaValue(gtin);
    const statusToMatch = mode === "Forwarding" ? "Ready to Forward" : "Available";

    if (mode === "Forwarding" && !sellerId) {
      return res.status(400).json({ error: "Missing seller_id for forwarding lookup" });
    }

    const sellerCode = mode === "Forwarding"
      ? await getSellerCodeByRecordId(sellerId)
      : "";

    const records = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
      .select({
        filterByFormula: mode === "Forwarding"
          ? `AND(
              TRIM({Product GTIN} & '') = '${safeGtin}',
              {Availability Status} = '${escapeFormulaValue(statusToMatch)}',
              FIND('${escapeFormulaValue(sellerCode)}', ARRAYJOIN({Seller ID})) > 0
            )`
          : `AND(
              TRIM({Product GTIN} & '') = '${safeGtin}',
              {Availability Status} = '${escapeFormulaValue(statusToMatch)}'
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
    const sellerIds = records
      .map((record) => Array.isArray(record.fields["Seller ID"]) ? record.fields["Seller ID"] : [])
      .flat();

    let averagePrice = 0;
    let totalPrice = 0;

    if (mode === "Forwarding") {
      averagePrice = await getAverageForwardingFeeForSellerIds(sellerIds);
      totalPrice = averagePrice * records.length;
    } else {
      const purchasePrices = records
        .map((r) => Number(r.fields["Purchase Price"]))
        .filter((n) => Number.isFinite(n));

      totalPrice = purchasePrices.reduce((sum, n) => sum + n, 0);
      averagePrice = purchasePrices.length ? totalPrice / purchasePrices.length : 0;
    }

    return res.status(200).json({
      found: true,
      gtin,
      product_name: productName,
      sku,
      size,
      available_quantity: records.length,
      unit_price: averagePrice,
      total_available_price: totalPrice,
      inventory_unit_ids: records.map((record) => record.id),
      seller_ids: sellerIds
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
    const mode = asText(req.body?.mode) || "Selling";
    const sellerId = asText(req.body?.seller_id);

    if (!sku || !size) {
      return res.status(400).json({ error: "Missing sku or size" });
    }

    const safeSku = escapeFormulaValue(sku);
    const safeSize = escapeFormulaValue(size);
    const statusToMatch = mode === "Forwarding" ? "Ready to Forward" : "Available";

    if (mode === "Forwarding" && !sellerId) {
      return res.status(400).json({ error: "Missing seller_id for forwarding search" });
    }

    const sellerCode = mode === "Forwarding"
      ? await getSellerCodeByRecordId(sellerId)
      : "";

    const records = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
      .select({
        filterByFormula: mode === "Forwarding"
          ? `AND(
              UPPER(TRIM({SKU} & '')) = '${safeSku}',
              TRIM({Size} & '') = '${safeSize}',
              {Availability Status} = '${escapeFormulaValue(statusToMatch)}',
              FIND('${escapeFormulaValue(sellerCode)}', ARRAYJOIN({Seller ID})) > 0
            )`
          : `AND(
              UPPER(TRIM({SKU} & '')) = '${safeSku}',
              TRIM({Size} & '') = '${safeSize}',
              {Availability Status} = '${escapeFormulaValue(statusToMatch)}'
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
    const sellerIds = records
      .map((record) => Array.isArray(record.fields["Seller ID"]) ? record.fields["Seller ID"] : [])
      .flat();

    let averagePrice = 0;
    let totalPrice = 0;

    if (mode === "Forwarding") {
      averagePrice = await getAverageForwardingFeeForSellerIds(sellerIds);
      totalPrice = averagePrice * records.length;
    } else {
      const purchasePrices = records
        .map((r) => Number(r.fields["Purchase Price"]))
        .filter((n) => Number.isFinite(n));

      totalPrice = purchasePrices.reduce((sum, n) => sum + n, 0);
      averagePrice = purchasePrices.length ? totalPrice / purchasePrices.length : 0;
    }

    return res.status(200).json({
      found: true,
      gtin,
      product_name: productName,
      sku,
      size,
      available_quantity: records.length,
      unit_price: averagePrice,
      total_available_price: totalPrice,
      inventory_unit_ids: records.map((record) => record.id),
      seller_ids: sellerIds,
      unit_forwarding_fee: averagePrice
    });
  } catch (error) {
    console.error("outbound-search-sku-size failed:", error);
    return res.status(500).json({
      error: "Failed to search outbound SKU/Size",
      details: error.message
    });
  }
});

app.post("/api/submit-outbound", async (req, res) => {
  try {
    const mode = asText(req.body?.mode);
    const buyerId = asText(req.body?.buyer_id);
    const sellerId = asText(req.body?.seller_id);
    const totalSellingPrice = Number(req.body?.total_selling_price) || 0;
    const shippingCosts = Number(req.body?.shipping_costs) || 0;
    const shippingLabels = Number(req.body?.shipping_labels) || 0;
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    if (mode !== "Selling" && mode !== "Forwarding") {
      return res.status(400).json({ error: "Invalid outbound mode" });
    }

    if (!items.length) {
      return res.status(400).json({ error: "No items submitted" });
    }

    const linkedInventoryUnitIds = items.flatMap((item) => {
      const quantity = Number(item?.quantity);
      const inventoryUnitIds = Array.isArray(item?.inventory_unit_ids) ? item.inventory_unit_ids : [];

      if (!Number.isInteger(quantity) || quantity < 1) {
        return [];
      }

      return inventoryUnitIds.slice(0, quantity);
    });

    if (!linkedInventoryUnitIds.length) {
      return res.status(400).json({ error: "No Inventory Unit record IDs found to submit" });
    }

    if (mode === "Selling") {
      if (!buyerId) {
        return res.status(400).json({ error: "Missing buyer_id" });
      }

      // Fetch Buyer ID from external Airtable
      const externalBuyerRecords = await buyersBase(BUYERS_AIRTABLE_TABLE)
        .select({
          fields: ["Buyer ID"],
          filterByFormula: `RECORD_ID() = '${escapeFormulaValue(buyerId)}'`,
          maxRecords: 1
        })
        .firstPage();
      
      const externalBuyer = externalBuyerRecords[0];
      if (!externalBuyer) {
        return res.status(400).json({ error: "Selected buyer not found" });
      }
      
      const buyerIdValue = asText(externalBuyer.fields["Buyer ID"]);
      
      // Find corresponding buyer in main Airtable
      const mainBuyerRecord = await findMainBuyerRecordByBuyerId(buyerIdValue);
      if (!mainBuyerRecord) {
        return res.status(400).json({
          error: `No matching buyer found in main Airtable for Buyer ID ${buyerIdValue}`
        });
      }
      
      const createdRecord = await airtable(AIRTABLE_EXTERNAL_SALES_LOG_TABLE).create({
        "Buyer ID": [mainBuyerRecord.id],
        "Linked Inventory Units": linkedInventoryUnitIds,
        "Total Selling Price": totalSellingPrice,
        "Shipping Costs": shippingCosts,
        "Amount of Labels": shippingLabels,
        "Sale Date": new Date().toISOString().split("T")[0]
      });

      await updateInventoryUnitsToReserved(linkedInventoryUnitIds);

      return res.status(200).json({
        ok: true,
        id: createdRecord.id,
        linked_inventory_units_count: linkedInventoryUnitIds.length
      });
    }

    if (!sellerId) {
      return res.status(400).json({ error: "Missing seller_id for forwarding" });
    }

    if (shippingLabels > 0 && !buyerId) {
      return res.status(400).json({ error: "Buyer is required when labels are needed" });
    }

    const forwardingUnitFees = items
      .map((item) => Number(item?.unit_forwarding_fee))
      .filter((value) => Number.isFinite(value));

    const averageForwardingFee = forwardingUnitFees.length
      ? forwardingUnitFees.reduce((sum, value) => sum + value, 0) / forwardingUnitFees.length
      : 0;

    const createFields = {
      "Seller ID": [sellerId],
      "Linked Inventory Units": linkedInventoryUnitIds,
      "Shipping Costs": shippingCosts,
      "Amount of Labels": shippingLabels,
      "Unit Forwarding Fee": averageForwardingFee,
      "Forwarding Date": new Date().toISOString().split("T")[0]
    };

    if (buyerId) {
      const externalBuyerRecords = await buyersBase(BUYERS_AIRTABLE_TABLE)
        .select({
          fields: ["Buyer ID"],
          filterByFormula: `RECORD_ID() = '${escapeFormulaValue(buyerId)}'`,
          maxRecords: 1
        })
        .firstPage();

      const externalBuyer = externalBuyerRecords[0];
      if (!externalBuyer) {
        return res.status(400).json({ error: "Selected buyer not found" });
      }

      const buyerIdValue = asText(externalBuyer.fields["Buyer ID"]);
      const mainBuyerRecord = await findMainBuyerRecordByBuyerId(buyerIdValue);

      if (!mainBuyerRecord) {
        return res.status(400).json({
          error: `No matching buyer found in main Airtable for Buyer ID ${buyerIdValue}`
        });
      }

      createFields["Buyer ID"] = [mainBuyerRecord.id];
    }

    const createdRecord = await airtable(AIRTABLE_FORWARDING_SERVICE_LOG_TABLE).create(createFields);

    await updateInventoryUnitsToForwardPending(linkedInventoryUnitIds);

    return res.status(200).json({
      ok: true,
      id: createdRecord.id,
      linked_inventory_units_count: linkedInventoryUnitIds.length
    });
  } catch (error) {
    console.error("submit-outbound failed:", error);
    return res.status(500).json({
      error: "Failed to submit outbound",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq WMS running on port ${PORT}`);
});
