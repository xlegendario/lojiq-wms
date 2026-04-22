import dotenv from "dotenv";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

dotenv.config();

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "10mb" }));

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
  BUYERS_AIRTABLE_TOKEN,
  AIRTABLE_OUTBOUND_SHIPPING_CODES_TABLE = "Outbound Shipping Codes",
  AIRTABLE_LABEL_REQUEST_ROUTING_TABLE = "Label Request Routing",
  SENDCLOUD_PUBLIC_KEY,
  SENDCLOUD_SECRET_KEY,
  SENDCLOUD_PARCELS_URL = "https://panel.sendcloud.sc/api/v2/parcels",
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE_URL,
  APP_PUBLIC_BASE_URL,
  DISCORD_BOT_BASE_URL
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

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY
  }
});

function asText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

const sellerCodeCache = new Map();

async function getSellerCodeFromRecordId(sellerRecordId) {
  if (!sellerRecordId) return "";

  if (sellerCodeCache.has(sellerRecordId)) {
    return sellerCodeCache.get(sellerRecordId);
  }

  const record = await airtable(AIRTABLE_SELLERS_TABLE).find(sellerRecordId);
  const sellerCode = asText(record.fields["Seller ID"]);

  sellerCodeCache.set(sellerRecordId, sellerCode);

  return sellerCode;
}

function escapeFormulaValue(value) {
  return asText(value).replace(/'/g, "\\'");
}

function buildBasicAuthHeader(publicKey, secretKey) {
  const token = Buffer.from(`${publicKey}:${secretKey}`).toString("base64");
  return `Basic ${token}`;
}

function sanitizeFileName(name) {
  return String(name || "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

function first(value) {
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

function splitStreetAndHouseNumber(addressLine1, addressLine2 = "") {
  const line1 = asText(addressLine1);
  const line2 = asText(addressLine2);

  if (!line1) {
    return {
      street: "",
      houseNumber: "",
      usedAddress2AsHouseNumber: false
    };
  }

  // 1. If address2 is only a house number, trust it first
  // Examples:
  // address1 = "Zuiderkeerkring"
  // address2 = "248"
  if (/^\d+[a-zA-Z0-9\-\/]*$/.test(line2)) {
    return {
      street: line1,
      houseNumber: line2,
      usedAddress2AsHouseNumber: true
    };
  }

  // 2. Standard format: "Kalverstraat 12", "Dorpsweg 15A", "Street 84-1"
  let match = line1.match(/^(.*\S)\s+(\d+[a-zA-Z0-9\-\/]*)$/);
  if (match) {
    return {
      street: asText(match[1]),
      houseNumber: asText(match[2]),
      usedAddress2AsHouseNumber: false
    };
  }

  // 3. Reverse format: "12 Kalverstraat", "15A Dorpsweg"
  match = line1.match(/^(\d+[a-zA-Z0-9\-\/]*)\s+(.*\S)$/);
  if (match) {
    return {
      street: asText(match[2]),
      houseNumber: asText(match[1]),
      usedAddress2AsHouseNumber: false
    };
  }

  // 4. Nothing reliable found
  return {
    street: line1,
    houseNumber: "",
    usedAddress2AsHouseNumber: false
  };
}

async function fetchBuffer(url, headers = {}) {
  const res = await fetch(url, { headers });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Failed to fetch buffer from ${url}: ${res.status} ${text}`);
  }

  return Buffer.from(await res.arrayBuffer());
}

async function uploadPdfToR2({ key, pdfBuffer }) {
  await r2.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: pdfBuffer,
      ContentType: "application/pdf"
    })
  );

  return `${R2_PUBLIC_BASE_URL}/${key}`;
}

async function getShopifyOrder({ shopDomain, accessToken, orderId }) {
  const url = `https://${shopDomain}/admin/api/2024-01/orders/${orderId}.json`;

  const res = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": accessToken,
      "Content-Type": "application/json"
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Shopify order fetch failed: ${res.status} ${text}`);
  }

  const data = await res.json();
  return data.order;
}

function extractCustomerAddress(shopifyOrder) {
  const addr = shopifyOrder?.shipping_address;

  if (!addr) {
    throw new Error("Shopify order missing shipping address");
  }

  const { street, houseNumber } = splitStreetAndHouseNumber(
    addr.address1 || "",
    addr.address2 || ""
  );

  return {
    name: `${addr.first_name || ""} ${addr.last_name || ""}`.trim(),
    company: addr.company || "",
    address1: street,
    houseNumber,
    address2: addr.address2 || "",
    city: addr.city || "",
    postalCode: addr.zip || "",
    country: addr.country_code || "",
    email: shopifyOrder?.email || "",
    phone: addr.phone || ""
  };
}

async function getOutboundShippingOptionCode(countryCode) {
  const safeCountryCode = escapeFormulaValue(countryCode);

  const records = await airtable(AIRTABLE_OUTBOUND_SHIPPING_CODES_TABLE)
    .select({
      fields: ["Country Code", "Shipping Option Code"],
      filterByFormula: `TRIM({Country Code} & '') = '${safeCountryCode}'`,
      maxRecords: 1
    })
    .firstPage();

  if (!records.length) {
    throw new Error(`No outbound shipping option configured for country ${countryCode}`);
  }

  const shippingOptionCode = asText(records[0].fields["Shipping Option Code"]);

  if (!shippingOptionCode) {
    throw new Error(`Shipping Option Code missing for country ${countryCode}`);
  }

  return shippingOptionCode;
}

function buildSendcloudOrderNumber(orderId, storeName, shopifyOrderNumber) {
  const cleanOrderId = asText(orderId)
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9-_]/g, "");

  const cleanStore = asText(storeName)
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9-_]/g, "");

  const cleanShopifyOrderNumber = asText(shopifyOrderNumber)
    .replace(/^#/, "") // verwijdert eventuele '#'
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9-_]/g, "");

  return [
    cleanOrderId,
    cleanStore,
    cleanShopifyOrderNumber
  ].filter(Boolean).join("-");
}

async function createSendcloudLabel({
  customerAddress,
  shippingOptionCode,
  orderId,
  storeName,
  shopifyOrderNumber
}) {
  const payload = {
    parcel: {
      name: customerAddress.name,
      company_name: customerAddress.company || undefined,
      address: customerAddress.address1,
      house_number: customerAddress.houseNumber,
      address_2: customerAddress.address2 || undefined,
      city: customerAddress.city,
      postal_code: customerAddress.postalCode,
      country: customerAddress.country,
      email: customerAddress.email || undefined,
      telephone: customerAddress.phone || undefined,
      shipment: {
        id: Number(shippingOptionCode)
      },
      request_label: true,
      apply_shipping_rules: false,
      weight: "0.5",
      order_number: buildSendcloudOrderNumber(
        orderId,
        storeName,
        shopifyOrderNumber
      )
    }
  };

  const res = await fetch(SENDCLOUD_PARCELS_URL, {
    method: "POST",
    headers: {
      Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY),
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const body = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Sendcloud create parcel failed: ${res.status} ${JSON.stringify(body)}`);
  }

  const parcel = body?.parcel || {};

  const rawLabelUrl =
    asText(parcel?.label?.normal_printer) ||
    asText(parcel?.label?.label_printer) ||
    asText(parcel?.label_url) ||
    "";

  const labelUrl = rawLabelUrl
    .split(",")
    .map((part) => asText(part))
    .filter(Boolean)[0] || "";

  const trackingNumber =
    asText(parcel?.tracking_number) ||
    asText(parcel?.tracking_no) ||
    "";

  if (!labelUrl) {
    throw new Error(`Sendcloud response missing label URL: ${JSON.stringify(body)}`);
  }

  if (!trackingNumber) {
    throw new Error(`Sendcloud response missing tracking number: ${JSON.stringify(body)}`);
  }

  return {
    labelUrl,
    trackingNumber
  };
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

// 👇 PASTE HERE
function isWarehouseItemFast(inventoryRecord, sellerCodeById) {
  const itemId = asText(inventoryRecord.fields["Item ID"]).toUpperCase();

  if (
    itemId.startsWith("PCS-") ||
    itemId.startsWith("KC-") ||
    itemId.startsWith("RSC-")
  ) {
    return true;
  }

  if (itemId.startsWith("OUT-")) {
    const allowedSellerCodes = ["SE-00537", "SE-00309", "SE-00781"];

    const sellerRecordIds = Array.isArray(inventoryRecord.fields["Seller ID"])
      ? inventoryRecord.fields["Seller ID"]
      : [];

    for (const sellerRecordId of sellerRecordIds) {
      const sellerCode = sellerCodeById?.get(sellerRecordId);

      if (allowedSellerCodes.includes(sellerCode)) {
        return true;
      }
    }
  }

  return false;
}

async function isWarehouseItem(inventoryRecord) {
  const itemId = asText(inventoryRecord?.fields["Item ID"]).toUpperCase();

  const sellerRecordIds = Array.isArray(inventoryRecord?.fields["Seller ID"])
    ? inventoryRecord.fields["Seller ID"]
    : [];

  // Always allowed
  if (
    itemId.startsWith("PCS-") ||
    itemId.startsWith("KC-") ||
    itemId.startsWith("RSC-")
  ) {
    return true;
  }

  // Conditional OUT-
  if (itemId.startsWith("OUT-")) {
    const allowedSellerCodes = ["SE-00537", "SE-00309", "SE-00781"];

    for (const sellerRecordId of sellerRecordIds) {
      const sellerCode = await getSellerCodeFromRecordId(sellerRecordId);

      if (allowedSellerCodes.includes(sellerCode)) {
        return true;
      }
    }

    return false;
  }

  return false;
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
        "Shipping Label",
        "Tracking Number"
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

  if (!unfulfilledInventoryIds.length) {
    return [...salesOptions, ...forwardingOptions];
  }
  
  for (let i = 0; i < unfulfilledInventoryIds.length; i += 50) {
    const batch = unfulfilledInventoryIds.slice(i, i + 50);
  
    const inventoryRecords = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
      .select({
        filterByFormula: `OR(${batch
          .map((id) => `RECORD_ID() = '${id}'`)
          .join(",")})`
      })
      .all();
  
    for (const record of inventoryRecords) {
      inventoryUnitsById.set(record.id, record);
    }
  }

  // 🔥 STEP 1: Collect all seller record IDs
  const allSellerRecordIds = new Set();
  
  for (const record of inventoryUnitsById.values()) {
    const sellerIds = Array.isArray(record.fields["Seller ID"])
      ? record.fields["Seller ID"]
      : [];
  
    sellerIds.forEach(id => allSellerRecordIds.add(id));
  }
  
  // 🔥 STEP 2: Fetch all sellers in batch
  const sellerCodeById = new Map();
  
  const sellerIdsArray = Array.from(allSellerRecordIds);
  
  for (let i = 0; i < sellerIdsArray.length; i += 50) {
    const batch = sellerIdsArray.slice(i, i + 50);
  
    const sellerRecords = await airtable(AIRTABLE_SELLERS_TABLE)
      .select({
        filterByFormula: `OR(${batch.map(id => `RECORD_ID()='${id}'`).join(",")})`
      })
      .all();
  
    for (const record of sellerRecords) {
      const code = asText(record.fields["Seller ID"]).trim().toUpperCase();
      sellerCodeById.set(record.id, code);
    }
  }
  
  const groupedOrders = new Map();

  for (const record of unfulfilledRecords) {
    const shopifyOrderNumber = asText(record.fields["Shopify Order Number"]);
    const trackingNumber = asText(record.fields["Tracking Number"]);
    const storeName = asText(record.fields["Store Name"]);
    const linkedInventoryUnitIds = Array.isArray(record.fields["Linked Inventory Unit"])
      ? record.fields["Linked Inventory Unit"]
      : [];
  
    const warehouseInventoryUnitIds = [];
  
    for (const id of linkedInventoryUnitIds) {
      const inventoryRecord = inventoryUnitsById.get(id);
      if (!inventoryRecord) continue;
  
      if (isWarehouseItemFast(inventoryRecord, sellerCodeById)) {
        warehouseInventoryUnitIds.push(id);
      }
    }
  
    if (!warehouseInventoryUnitIds.length) continue;
  
    const hasTrackingNumber = !!trackingNumber;
    const groupKey = hasTrackingNumber
      ? `tracking||${trackingNumber}||${storeName}`
      : `shopify||${shopifyOrderNumber}||${storeName}`;
  
    if (!groupedOrders.has(groupKey)) {
      groupedOrders.set(groupKey, {
        id: groupKey,
        source_table: "unfulfilled_orders_log",
        shipping_status: asText(record.fields["Shipping Status"]) || "Pending",
        tracking_numbers_count: 1,
        store_name: storeName || "Unknown Store",
        order_numbers: new Set()
      });
    }
  
    if (shopifyOrderNumber) {
      groupedOrders.get(groupKey).order_numbers.add(shopifyOrderNumber);
    }
  }
  
  const unfulfilledOptions = Array.from(groupedOrders.values()).map((group) => {
    const orderNumbers = Array.from(group.order_numbers);
    const orderNumbersText = orderNumbers.length
      ? orderNumbers.join(", ")
      : "No Order Number";
  
    return {
      id: group.id,
      source_table: group.source_table,
      label: `${group.store_name} / ${orderNumbersText}`,
      shipping_status: group.shipping_status,
      tracking_numbers_count: group.tracking_numbers_count
    };
  });
  
  return [...salesOptions, ...forwardingOptions, ...unfulfilledOptions];
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

async function getSellerCountryCodeFromOrderFields(orderFields) {
  const linkedSellerIds = Array.isArray(orderFields["Linked Seller ID"])
    ? orderFields["Linked Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  const claimedSellerIds = Array.isArray(orderFields["Claimed Seller ID"])
    ? orderFields["Claimed Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  const sellerRecordId = linkedSellerIds[0] || claimedSellerIds[0] || "";

  if (!sellerRecordId) {
    return "";
  }

  const sellerRecord = await airtable(AIRTABLE_SELLERS_TABLE).find(sellerRecordId);
  return asText(sellerRecord.fields["Country Code"]);
}

async function getPreferredCourierForCountryCode(countryCode) {
  const safeCountryCode = escapeFormulaValue(countryCode);

  if (!safeCountryCode) {
    return {
      preferredCourier: "",
      instructionText: ""
    };
  }

  const records = await airtable(AIRTABLE_LABEL_REQUEST_ROUTING_TABLE)
    .select({
      fields: ["Country Code", "Preferred Courier", "Instruction Text"],
      filterByFormula: `TRIM({Country Code} & '') = '${safeCountryCode}'`,
      maxRecords: 1
    })
    .firstPage();

  if (!records.length) {
    return {
      preferredCourier: "",
      instructionText: ""
    };
  }

  return {
    preferredCourier: asText(records[0].fields["Preferred Courier"]),
    instructionText: asText(records[0].fields["Instruction Text"])
  };
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

async function markUnfulfilledOrderLabelError(recordId, errorMessage) {
  await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(recordId, {
    "Fulfillment Status": "Label Error",
    "Label Error Message": asText(errorMessage)
  });
}

async function sendFinalLabelToDiscordChannel({
  channelId,
  orderId,
  trackingNumber,
  labelUrl
}) {
  if (!process.env.DISCORD_TOKEN) {
    throw new Error("Missing DISCORD_TOKEN");
  }

  const url = `https://discord.com/api/v10/channels/${channelId}/messages`;

  const body = {
    embeds: [
      {
        title: "📦 Shipping Label Ready",
        color: 0x00b894,
        description:
          `**Order:** ${orderId}\n` +
          `**Tracking:** ${trackingNumber}\n\n` +
          `[📄 Download Label](${labelUrl})`,
        footer: {
          text: "Kickz Caviar"
        }
      }
    ]
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bot ${process.env.DISCORD_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const text = await response.text().catch(() => "");

  if (!response.ok) {
    throw new Error(`Discord API error: ${response.status} ${text}`);
  }

  await markChannelLabelOk(channelId);

  return true;
}

async function markChannelLabelOk(channelId) {
  if (!process.env.DISCORD_TOKEN) {
    throw new Error("Missing DISCORD_TOKEN");
  }

  const url = `https://discord.com/api/v10/channels/${channelId}`;

  const getResponse = await fetch(url, {
    method: "GET",
    headers: {
      "Authorization": `Bot ${process.env.DISCORD_TOKEN}`,
      "Content-Type": "application/json"
    }
  });

  const getText = await getResponse.text().catch(() => "");
  let channelData = {};

  try {
    channelData = getText ? JSON.parse(getText) : {};
  } catch {
    channelData = {};
  }

  if (!getResponse.ok) {
    throw new Error(`Failed to load channel: ${getResponse.status} ${getText}`);
  }

  const currentName = asText(channelData.name).toLowerCase();

  if (!currentName) {
    return;
  }

  if (currentName.endsWith("-labelok")) {
    return;
  }

  let newName = `${currentName}-labelok`;

  if (newName.length > 100) {
    newName = newName.slice(0, 100);
  }

  const patchResponse = await fetch(url, {
    method: "PATCH",
    headers: {
      "Authorization": `Bot ${process.env.DISCORD_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      name: newName
    })
  });

  const patchText = await patchResponse.text().catch(() => "");

  if (!patchResponse.ok) {
    throw new Error(`Failed to rename channel: ${patchResponse.status} ${patchText}`);
  }
}

async function postLabelRequestToDiscordBot({
  channelId,
  recordId,
  orderId,
  shopifyOrderNumber,
  productName,
  sku,
  size,
  storeName,
  labelRequestUrl,
  sellerCountryCode,
  preferredCourier,
  courierInstruction
}) {
  if (!DISCORD_BOT_BASE_URL) {
    throw new Error("Missing DISCORD_BOT_BASE_URL");
  }

  const url = `${DISCORD_BOT_BASE_URL.replace(/\/$/, "")}/post-label-request`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      channel_id: channelId,
      record_id: recordId,
      order_id: orderId,
      shopify_order_number: shopifyOrderNumber,
      product_name: productName,
      sku,
      size,
      store_name: storeName,
      label_request_url: labelRequestUrl,
      seller_country_code: sellerCountryCode,
      preferred_courier: preferredCourier,
      courier_instruction: courierInstruction
    })
  });

  const rawText = await response.text().catch(() => "");
  let data = {};

  try {
    data = rawText ? JSON.parse(rawText) : {};
  } catch {
    data = {};
  }

  if (!response.ok) {
    throw new Error(
      data.details ||
      data.error ||
      `Discord bot returned ${response.status}: ${rawText || "No response body"}`
    );
  }

  return data;
}
async function getUnfulfilledOrderRecordById(recordId) {
  return airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).find(recordId);
}

function pdfBufferFromDataUrl(dataUrl) {
  const value = asText(dataUrl);

  const match = value.match(/^data:application\/pdf;base64,(.+)$/);

  if (!match) {
    throw new Error("Invalid PDF upload format");
  }

  return Buffer.from(match[1], "base64");
}

async function updateUnfulfilledOrderManualLabel({
  recordId,
  orderId,
  trackingNumber,
  pdfBuffer,
  originalFileName
}) {
  const safeOrderId = sanitizeFileName(orderId || recordId || "label");
  const safeFileName = sanitizeFileName(originalFileName || `${safeOrderId}.pdf`);
  const r2Key = `shipping-labels/manual-${safeOrderId}-${Date.now()}-${safeFileName}`;

  const uploadedPdfUrl = await uploadPdfToR2({
    key: r2Key,
    pdfBuffer
  });

  await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(recordId, {
    "Tracking Number": trackingNumber,
    "Shipping Label": [
      {
        url: uploadedPdfUrl,
        filename: safeFileName.endsWith(".pdf") ? safeFileName : `${safeFileName}.pdf`
      }
    ],
    "Shipping Label URL (Permanent)": uploadedPdfUrl,
    "Fulfillment Status": "Ready to Ship",
    "Label Error Message": null
  });
  return uploadedPdfUrl;
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
    const [groupType, groupValue, storeName] = outboundId.split("||");
  
    if (!groupType || !groupValue) {
      throw new Error("Invalid unfulfilled outbound id");
    }
  
    const groupFilter =
      groupType === "tracking"
        ? `TRIM({Tracking Number} & '') = '${escapeFormulaValue(groupValue)}'`
        : `TRIM({Shopify Order Number} & '') = '${escapeFormulaValue(groupValue)}'`;
  
    const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
      .select({
        fields: [
          "Shopify Order Number",
          "Tracking Number",
          "Store Name",
          "Fulfillment Status",
          "Shipping Status",
          "Linked Inventory Unit",
          "Shipping Label"
        ],
        filterByFormula: `AND(
          ${groupFilter},
          TRIM({Store Name} & '') = '${escapeFormulaValue(storeName || "")}',
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

    const filteredItems = [];

    for (const itemRecord of inventoryUnitRecords) {
      if (await isWarehouseItem(itemRecord)) {
        filteredItems.push(itemRecord);
      }
    }
    
    const items = filteredItems.map((itemRecord) => ({
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
      tracking_numbers: [groupType === "tracking" ? groupValue : "ORDER"],
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
  let matchedOrderRecordId = "";
  let matchedOrderId = "";

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
          "GOAT Tracking Number",
          "Client",
          "Shopify Order ID",
          "Shopify Order Number",
          "Store Name",
          "Product Name",
          "SKU",
          "Size",
          "Shipping Label",
          "Tracking Number"
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

      // Only process first match safely
      const orderRecord = unfulfilledRecords[0];
      const orderFields = orderRecord.fields || {};
      const orderId = asText(orderFields["Order ID"]) || orderRecord.id;

      matchedOrderRecordId = orderRecord.id;
      matchedOrderId = orderId;

      const clientId = first(orderFields["Client"]);
      if (!clientId) {
        throw new Error(`The Order ${orderId} has no linked Client`);
      }

      const merchantRecord = await airtable(AIRTABLE_MERCHANTS_TABLE).find(clientId);
      const merchantFields = merchantRecord.fields || {};
      
      const labelsOnContract = !!merchantFields["Labels On Contract?"];
      const labelRequestChannelId = asText(merchantFields["Label Request Channel ID"]);
      const sendcloudSenderAddressId = asText(merchantFields["Sendcloud Sender Address ID"]);
      const senderDisplayName = asText(merchantFields["Sender Display Name"]);

      if (!labelsOnContract) {
        if (!labelRequestChannelId) {
          throw new Error(`Missing Label Request Channel ID for merchant linked to order ${orderId}`);
        }
      
        if (!APP_PUBLIC_BASE_URL) {
          throw new Error("Missing APP_PUBLIC_BASE_URL");
        }
      
        const productName = asText(orderFields["Product Name"]);
        const sku = asText(orderFields["SKU"]);
        const size = asText(orderFields["Size"]);
        const shopifyOrderNumber = asText(orderFields["Shopify Order Number"]);
        const labelRequestUrl = `${asText(APP_PUBLIC_BASE_URL).replace(/\/$/, "")}/label-request.html?record_id=${encodeURIComponent(orderRecord.id)}`;
      
        await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(orderRecord.id, {
          "Fulfillment Status": "Requested Label",
          "Label Error Message": null
        });

        const sellerCountryCode = await getSellerCountryCodeFromOrderFields(orderFields);

        const { preferredCourier, instructionText } =
          await getPreferredCourierForCountryCode(sellerCountryCode);
        
        const courierInstruction =
          instructionText ||
          (preferredCourier ? `Please provide a ${preferredCourier} label.` : "");
        
        await postLabelRequestToDiscordBot({
          channelId: labelRequestChannelId,
          recordId: orderRecord.id,
          orderId,
          shopifyOrderNumber,
          productName,
          sku,
          size,
          storeName: asText(orderFields["Store Name"]) || asText(merchantFields["Store Name"]),
          labelRequestUrl,
          sellerCountryCode,
          preferredCourier,
          courierInstruction
        });
      
        return res.status(200).json({
          message: `Label request registered for ${orderId}`,
          exists: false,
          matched_unfulfilled_order: true,
          order_id: orderId,
          label_request_url: labelRequestUrl
        });
      }

      const shopDomain = asText(merchantFields["Shopify Store URL"])
        .replace(/^https?:\/\//, "")
        .replace(/\/$/, "");

      const accessToken = asText(merchantFields["Shopify Token"]);
      const shopifyOrderId = asText(orderFields["Shopify Order ID"]);
      const storeName = asText(orderFields["Store Name"]) || asText(merchantFields["Store Name"]);
      const shopifyOrderNumber = asText(orderFields["Shopify Order Number"]);

      if (!shopDomain) {
        throw new Error(`Missing Shopify Store URL for merchant linked to order ${orderId}`);
      }

      if (!accessToken) {
        throw new Error(`Missing Shopify Token for merchant linked to order ${orderId}`);
      }

      if (!shopifyOrderId) {
        throw new Error(`Missing Shopify Order ID on order ${orderId}`);
      }

      const shopifyOrder = await getShopifyOrder({
        shopDomain,
        accessToken,
        orderId: shopifyOrderId
      });

      const customerAddress = extractCustomerAddress(shopifyOrder);

      if (!customerAddress.houseNumber) {
        throw new Error(`Customer address is missing a detectable house number for order ${orderId}`);
      }

      const shippingOptionCode = await getOutboundShippingOptionCode(customerAddress.country);

      const sendcloud = await createSendcloudLabel({
        customerAddress,
        shippingOptionCode,
        orderId,
        storeName,
        shopifyOrderNumber
      });

      const labelPdfBuffer = await fetchBuffer(sendcloud.labelUrl, {
        Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
      });

      const r2Key = `shipping-labels/${sanitizeFileName(orderId)}.pdf`;
      const uploadedPdfUrl = await uploadPdfToR2({
        key: r2Key,
        pdfBuffer: labelPdfBuffer
      });

      await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(orderRecord.id, {
        "Fulfillment Status": "Requested Label",
        "Tracking Number": sendcloud.trackingNumber,
        "Shipping Label": [
          {
            url: uploadedPdfUrl,
            filename: `${sanitizeFileName(orderId)}.pdf`
          }
        ],
        "Shipping Label URL (Permanent)": uploadedPdfUrl,
        "Label Error Message": null
      });

      return res.status(200).json({
        message: `Label is requested for ${orderId} successfully`,
        exists: false,
        matched_unfulfilled_order: true,
        order_id: orderId
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

    if (matchedOrderRecordId) {
      try {
        await markUnfulfilledOrderLabelError(matchedOrderRecordId, error.message);
      } catch (updateError) {
        console.error("Failed to write label error back to Airtable:", updateError);
      }
    }

    return res.status(500).json({
      error: matchedOrderId
        ? `Label generation failed for ${matchedOrderId}`
        : "Failed to process parcel",
      details: error.message
    });
  }
});

app.get("/api/label-request-order/:recordId", async (req, res) => {
  try {
    const recordId = asText(req.params?.recordId);

    if (!recordId) {
      return res.status(400).json({ error: "Missing recordId" });
    }

    const record = await getUnfulfilledOrderRecordById(recordId);
    const fields = record.fields || {};

    return res.status(200).json({
      ok: true,
      order: {
        record_id: record.id,
        order_id: asText(fields["Order ID"]),
        shopify_order_number: asText(fields["Shopify Order Number"]),
        product_name: asText(fields["Product Name"]),
        size: asText(fields["Size"]),
        sku: asText(fields["SKU"]),
        store_name: asText(fields["Store Name"]),
        fulfillment_status: asText(fields["Fulfillment Status"]),
        tracking_number: asText(fields["Tracking Number"])
      }
    });
  } catch (error) {
    console.error("label-request-order failed:", error);
    return res.status(500).json({
      error: "Failed to load label request order",
      details: error.message
    });
  }
});

app.post("/api/label-request-submit", async (req, res) => {
  try {
    const recordId = asText(req.body?.record_id);
    const trackingNumber = asText(req.body?.tracking_number);
    const fileName = asText(req.body?.file_name);
    const fileDataUrl = asText(req.body?.file_data_url);

    if (!recordId) {
      return res.status(400).json({ error: "Missing record_id" });
    }

    if (!trackingNumber) {
      return res.status(400).json({ error: "Missing tracking_number" });
    }

    if (!fileName) {
      return res.status(400).json({ error: "Missing file_name" });
    }

    if (!fileDataUrl) {
      return res.status(400).json({ error: "Missing file_data_url" });
    }

    const record = await getUnfulfilledOrderRecordById(recordId);
    const fields = record.fields || {};
    const orderId = asText(fields["Order ID"]) || record.id;

    const pdfBuffer = pdfBufferFromDataUrl(fileDataUrl);

    await updateUnfulfilledOrderManualLabel({
      recordId,
      orderId,
      trackingNumber,
      pdfBuffer,
      originalFileName: fileName
    });

    return res.status(200).json({
      ok: true,
      message: `Label saved for ${orderId}`
    });
  } catch (error) {
    console.error("label-request-submit failed:", error);

    const recordId = asText(req.body?.record_id);
    if (recordId) {
      try {
        await markUnfulfilledOrderLabelError(recordId, error.message);
      } catch (updateError) {
        console.error("Failed to save label request error:", updateError);
      }
    }

    return res.status(500).json({
      error: "Failed to submit manual label request",
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
      const [groupType, groupValue, storeName] = outboundId.split("||");
    
      if (!groupType || !groupValue) {
        return res.status(400).json({ error: "Invalid unfulfilled outbound_id" });
      }
    
      const groupFilter =
        groupType === "tracking"
          ? `TRIM({Tracking Number} & '') = '${escapeFormulaValue(groupValue)}'`
          : `TRIM({Shopify Order Number} & '') = '${escapeFormulaValue(groupValue)}'`;
    
      const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
        .select({
          fields: [
            "Shopify Order Number",
            "Tracking Number",
            "Store Name",
            "Fulfillment Status",
            "Shipping Status",
            "Linked Inventory Unit"
          ],
          filterByFormula: `AND(
            ${groupFilter},
            TRIM({Store Name} & '') = '${escapeFormulaValue(storeName || "")}',
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
    
        let hasWarehouseItem = false;
    
        for (const itemRecord of inventoryUnitRecords) {
          if (await isWarehouseItem(itemRecord)) {
            hasWarehouseItem = true;
            break;
          }
        }
    
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
              "Fulfillment Status": "Fulfilled"
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

app.post("/api/request-label", async (req, res) => {
  try {
    const source = asText(req.body?.source);
    const recordId = asText(req.body?.record_id);

    if (!source) {
      return res.status(400).json({ error: "Missing source" });
    }

    if (!recordId) {
      return res.status(400).json({ error: "Missing record_id" });
    }

    if (source !== "quick_deal") {
      return res.status(400).json({ error: "Unsupported source" });
    }

    const orderRecord = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).find(recordId);
    const orderFields = orderRecord.fields || {};
    const orderId = asText(orderFields["Order ID"]) || orderRecord.id;

    const existingTrackingNumber = asText(orderFields["Tracking Number"]);
    const existingShippingLabel = Array.isArray(orderFields["Shipping Label"])
      ? orderFields["Shipping Label"]
      : [];
    
    if (existingTrackingNumber || existingShippingLabel.length > 0) {
      return res.status(400).json({
        error: `A label already exists for ${orderId}`
      });
    }

    const clientId = first(orderFields["Client"]);
    if (!clientId) {
      throw new Error(`The Order ${orderId} has no linked Client`);
    }

    const claimedChannelId = asText(orderFields["Claimed Channel ID"]);
    if (!claimedChannelId) {
      throw new Error(`Missing Claimed Channel ID for order ${orderId}`);
    }

    const merchantRecord = await airtable(AIRTABLE_MERCHANTS_TABLE).find(clientId);
    const merchantFields = merchantRecord.fields || {};

    const labelsOnContract = !!merchantFields["Labels On Contract?"];
    const labelRequestChannelId = asText(merchantFields["Label Request Channel ID"]);

    if (!labelsOnContract) {
      if (!labelRequestChannelId) {
        throw new Error(`Missing Label Request Channel ID for merchant linked to order ${orderId}`);
      }

      if (!APP_PUBLIC_BASE_URL) {
        throw new Error("Missing APP_PUBLIC_BASE_URL");
      }

      const labelRequestUrl =
        `${asText(APP_PUBLIC_BASE_URL).replace(/\/$/, "")}/label-request.html?record_id=${encodeURIComponent(orderRecord.id)}`;

      await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(orderRecord.id, {
        "Fulfillment Status": "Requested Label",
        "Label Error Message": null
      });

      const sellerCountryCode = await getSellerCountryCodeFromOrderFields(orderFields);

      const { preferredCourier, instructionText } =
        await getPreferredCourierForCountryCode(sellerCountryCode);
      
      const courierInstruction =
        instructionText ||
        (preferredCourier ? `Please provide a ${preferredCourier} label.` : "");

      await postLabelRequestToDiscordBot({
        channelId: labelRequestChannelId,
        recordId: orderRecord.id,
        orderId,
        shopifyOrderNumber: asText(orderFields["Shopify Order Number"]),
        productName: asText(orderFields["Product Name"]),
        sku: asText(orderFields["SKU"]),
        size: asText(orderFields["Size"]),
        storeName: asText(orderFields["Store Name"]) || asText(merchantFields["Store Name"]),
        labelRequestUrl,
        sellerCountryCode,
        preferredCourier,
        courierInstruction
      });

      return res.status(200).json({
        ok: true,
        message: `Manual label request sent for ${orderId}`
      });
    }

    const shopDomain = asText(merchantFields["Shopify Store URL"])
      .replace(/^https?:\/\//, "")
      .replace(/\/$/, "");

    const accessToken = asText(merchantFields["Shopify Token"]);
    const shopifyOrderId = asText(orderFields["Shopify Order ID"]);
    const storeName = asText(orderFields["Store Name"]) || asText(merchantFields["Store Name"]);
    const shopifyOrderNumber = asText(orderFields["Shopify Order Number"]);

    if (!shopDomain) throw new Error(`Missing Shopify Store URL for merchant linked to order ${orderId}`);
    if (!accessToken) throw new Error(`Missing Shopify Token for merchant linked to order ${orderId}`);
    if (!shopifyOrderId) throw new Error(`Missing Shopify Order ID on order ${orderId}`);

    const shopifyOrder = await getShopifyOrder({
      shopDomain,
      accessToken,
      orderId: shopifyOrderId
    });

    const customerAddress = extractCustomerAddress(shopifyOrder);

    if (!customerAddress.houseNumber) {
      throw new Error(`Customer address is missing a detectable house number for order ${orderId}`);
    }

    const shippingOptionCode = await getOutboundShippingOptionCode(customerAddress.country);

    const sendcloud = await createSendcloudLabel({
      customerAddress,
      shippingOptionCode,
      orderId,
      storeName,
      shopifyOrderNumber
    });

    const labelPdfBuffer = await fetchBuffer(sendcloud.labelUrl, {
      Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
    });

    const r2Key = `shipping-labels/${sanitizeFileName(orderId)}.pdf`;
    const uploadedPdfUrl = await uploadPdfToR2({
      key: r2Key,
      pdfBuffer: labelPdfBuffer
    });

    await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(orderRecord.id, {
      "Fulfillment Status": "Requested Label",
      "Tracking Number": sendcloud.trackingNumber,
      "Shipping Label": [
        {
          url: uploadedPdfUrl,
          filename: `${sanitizeFileName(orderId)}.pdf`
        }
      ],
      "Label Error Message": null
    });

    await sendFinalLabelToDiscordChannel({
      channelId: claimedChannelId,
      orderId,
      trackingNumber: sendcloud.trackingNumber,
      labelUrl: uploadedPdfUrl
    });

    return res.status(200).json({
      ok: true,
      message: `Label created for ${orderId}`
    });
  } catch (error) {
    console.error("request-label failed:", error);
    return res.status(500).json({
      error: "Failed to request label",
      details: error.message
    });
  }
});

app.post("/send-label-to-channel", async (req, res) => {
  try {
    const recordId = req.body?.recordId;

    if (!recordId) {
      return res.status(400).json({ error: "Missing recordId" });
    }

    const record = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).find(recordId);
    const fields = record.fields || {};

    const claimedChannelId = asText(fields["Claimed Channel ID"]);
    const wtbChannelId = asText(fields["WTB Created Channel ID"]);
    const targetChannelId = claimedChannelId || wtbChannelId;

    if (!targetChannelId) {
      throw new Error("No channel ID found");
    }

    const permanentLabelUrl = asText(fields["Shipping Label URL (Permanent)"]);
    const labelField = fields["Shipping Label"];
    
    const labelUrl = permanentLabelUrl || (
      Array.isArray(labelField) && labelField.length > 0
        ? labelField[0].url
        : null
    );

    const trackingNumber = asText(fields["Tracking Number"]);

    if (!labelUrl) {
      throw new Error("No label URL found");
    }

    // 👇 gebruik je bestaande functie
    await sendFinalLabelToDiscordChannel({
      channelId: targetChannelId,
      orderId: asText(fields["Order ID"]) || record.id,
      trackingNumber,
      labelUrl
    });

    // 👇 voorkom dubbele sends
    await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(recordId, {
      "Label Sent To Discord?": true
    });

    return res.json({ ok: true });

  } catch (error) {
    console.error("send-label-to-channel failed:", error);
    return res.status(500).json({
      error: "Failed to send label",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq WMS running on port ${PORT}`);
});
