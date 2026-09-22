import dotenv from "dotenv";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { PDFDocument } from "pdf-lib";
import returnsRouter from "./src/routes/returns.js";

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
  AIRTABLE_RETURNS_TABLE = "Incoming Returns",
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
  SENDCLOUD_SHIPPING_METHODS_URL = "https://panel.sendcloud.sc/api/v2/shipping_methods",
  /*
    The sender a marketplace label is made out to.

    Always our Dutch address, whatever country the consignor ships from:
    that is what the courier routing exists for, because DPD only accepts
    these parcels abroad against a Dutch sender while UPS takes them from
    anywhere. Its own id rather than the account default, so the name on a
    bol parcel can read UNION Amsterdam without renaming every store label
    and every return at the same time.
  */
  SENDCLOUD_MARKETPLACE_SENDER_ADDRESS_ID = "",
  /*
    What a boxed pair actually weighs, declared rather than guessed.

    It picks the method as well as paying for it: most UPS tariffs are one
    method per half kilo, so a parcel declared at half a kilo is quoted in
    the wrong band and re-weighed by the courier at their own price.
  */
  SENDCLOUD_MARKETPLACE_WEIGHT_KG = "1.5",
  /*
    The number a marketplace label carries when the shopper gave none.

    bol hands over no phone number at all, and UPS refuses to announce a
    parcel without one: the first bol pair shipped from outside the
    Netherlands came back "Announcement failed" for exactly that. Ours, so a
    courier who needs to call reaches someone who can act on it.
  */
  SENDCLOUD_MARKETPLACE_FALLBACK_PHONE = "+31634349800",
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE_URL,
  APP_PUBLIC_BASE_URL,
  DISCORD_BOT_BASE_URL,
  KICKZ_PORTAL_BASE_URL = "https://kickz-caviar-portal.onrender.com"
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

/*
 * The shopper's address, read off our own order instead of out of Shopify.
 *
 * A marketplace order has no Shopify order behind it to look anything up
 * in, so bol's address travels on the record from the moment the sale is
 * read. The street and the house number arrive as one line there, which is
 * the one thing that has to be taken apart again: Sendcloud refuses a
 * parcel without a house number of its own.
 */
function customerAddressFromOrderFields(orderFields) {
  const line = asText(orderFields["Customer Address"]);

  const { street, houseNumber } = splitStreetAndHouseNumber(line, "");

  return {
    name: asText(orderFields["Customer Name"]),
    company: "",
    address1: street || line,
    houseNumber,
    address2: "",
    city: asText(orderFields["Customer City"]),
    postalCode: asText(orderFields["Customer Zipcode"]),
    country: asText(orderFields["Customer Country"]).toUpperCase(),
    email: asText(orderFields["Customer Email"]),
    phone: ""
  };
}

/*
 * Which courier carries this one, decided by where the consignor sits.
 *
 * Label Request Routing already answers this for a human, in a sentence.
 * The same row answers it for us: a country that reads UPS/DPD can do both
 * against our Dutch sender, and where it may, it always does. Everywhere
 * else only UPS reaches, which is why those rows say so and why UPS is the
 * fallback when a country has no row at all.
 */
async function pickMarketplaceCarrier(sellerCountryCode) {
  const { preferredCourier } = await getPreferredCourierForCountryCode(
    sellerCountryCode
  );

  return /dpd/i.test(asText(preferredCourier)) ? "DPD" : "UPS";
}

/*
 * Which Sendcloud method each courier means, by name.
 *
 * One service per courier, and deliberately not the cheapest one on offer:
 * what Sendcloud quotes is not what we pay, because both couriers bill on
 * our own contract the moment their method is picked. So the cheapest-looking
 * option is a number with no bearing on the invoice, and choosing on it would
 * quietly move parcels onto a service nobody agreed to - a pick-up point the
 * shopper never chose, or a drop-off the consignor cannot reach.
 */
const MARKETPLACE_METHOD_BY_CARRIER = {
  DPD: "DPD Home",
  UPS: "UPS Standard"
};

/*
 * Sendcloud's own id for that service to that country.
 *
 * Asked rather than kept in a table. The Outbound Shipping Codes table holds
 * one id for three countries and knows nothing about couriers, so it cannot
 * express "DPD where DPD reaches" - and a table of ids is wrong the day
 * Sendcloud renumbers a contract, silently, on the next label.
 *
 * Matched on the exact name, because the list is full of near misses: "UPS
 * Standard 1-2kg", "UPS Standard - Signature" and "UPS® Standard" all read
 * as UPS Standard to anything looser than this.
 */
async function findSendcloudShippingMethod({
  carrier,
  toCountry,
  senderAddressId,
  weightKg = Number(SENDCLOUD_MARKETPLACE_WEIGHT_KG)
}) {
  const wantedName = MARKETPLACE_METHOD_BY_CARRIER[carrier];

  if (!wantedName) throw new Error(`No Sendcloud method configured for ${carrier}`);

  const params = new URLSearchParams({ to_country: asText(toCountry) });

  if (senderAddressId) params.set("sender_address", String(senderAddressId));

  const res = await fetch(`${SENDCLOUD_SHIPPING_METHODS_URL}?${params}`, {
    headers: {
      Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
    }
  });

  const body = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(
      `Sendcloud shipping methods failed: ${res.status} ${JSON.stringify(body)}`
    );
  }

  const methods = Array.isArray(body?.shipping_methods) ? body.shipping_methods : [];

  const wanted = asText(toCountry).toUpperCase();

  const onLane = (method) =>
    asText(method.carrier).toLowerCase() === carrier.toLowerCase() &&
    (method.countries || []).some(
      (country) => asText(country.iso_2).toUpperCase() === wanted
    );

  /*
    The plain name on our own contract, a weight band on Sendcloud's.

    Our contract offers "UPS Standard" for any weight. With it switched off
    (blocked by UPS in September 2026) the same service comes from Sendcloud
    as "UPS Standard 1-2kg", "UPS Standard 2-3kg" and so on, so the plain
    name finds nothing. The band that holds the declared weight is the same
    service at Sendcloud's price - lower bound included, upper excluded, the
    way their bands are cut.
  */
  const bandOf = (method) => {
    const name = asText(method.name);
    const prefix = `${wantedName} `;

    if (!name.startsWith(prefix)) return null;

    const found = /^(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)kg$/.exec(name.slice(prefix.length));

    return found ? { from: Number(found[1]), to: Number(found[2]) } : null;
  };

  const match =
    methods.find((method) => asText(method.name) === wantedName && onLane(method)) ||
    methods.find((method) => {
      const band = bandOf(method);

      return band && onLane(method) && weightKg >= band.from && weightKg < band.to;
    });

  if (!match) {
    // Names only change with the contract behind them, so say what IS on
    // offer for this carrier: that is the whole answer to "what now".
    const offered = methods
      .filter((method) => asText(method.carrier).toLowerCase() === carrier.toLowerCase())
      .filter((method) => (method.countries || []).some((country) => asText(country.iso_2).toUpperCase() === wanted))
      .map((method) => `${method.name} (#${method.id})`);

    throw new Error(
      `Sendcloud does not offer "${wantedName}" to ${toCountry}` +
        (senderAddressId ? ` from sender address ${senderAddressId}` : "") +
        `. ${carrier} methods on offer: ${offered.length ? offered.join(", ") : "none"}`
    );
  }

  /*
    A parcel outside the band would be refused at creation, and DPD Home
    stops well before UPS Standard does.
  */
  if (
    Number.isFinite(weightKg) &&
    (weightKg < Number(match.min_weight) || weightKg > Number(match.max_weight))
  ) {
    throw new Error(
      `"${wantedName}" carries ${match.min_weight}kg to ${match.max_weight}kg, ` +
        `and this parcel is declared at ${weightKg}kg`
    );
  }

  return { id: match.id, name: asText(match.name) };
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
  shopifyOrderNumber,
  senderAddressId = "",
  weightKg = null,
  fallbackPhone = "",
  contractId = null
}) {
  const payload = {
    parcel: {
      // Both left out for a store order, which keeps the account default
      // sender and the weight this function has always sent.
      sender_address: senderAddressId ? Number(senderAddressId) : undefined,
      name: customerAddress.name,
      company_name: customerAddress.company || undefined,
      address: customerAddress.address1,
      house_number: customerAddress.houseNumber,
      address_2: customerAddress.address2 || undefined,
      city: customerAddress.city,
      postal_code: customerAddress.postalCode,
      country: customerAddress.country,
      email: customerAddress.email || undefined,
      telephone: customerAddress.phone || fallbackPhone || undefined,
      shipment: {
        id: Number(shippingOptionCode)
      },
      request_label: true,
      apply_shipping_rules: false,
      // Only when asked for: which carrier contract pays, e.g. Sendcloud's
      // own rates instead of ours while our UPS contract is blocked.
      ...(contractId ? { contract: Number(contractId) } : {}),
      weight: weightKg ? String(weightKg) : "0.5",
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

/*
 * Tracking numbers as typed: one per row, or several in one field separated
 * by commas, spaces or new lines. Duplicates dropped, order kept.
 */
function trackingList(value) {
  const raw = Array.isArray(value) ? value.join(",") : asText(value);

  // One per line or separated by commas - never by spaces: a UPS number is
  // often typed with them ("1Z FV6 483 68 2567 1031") and must stay one
  // number. The spaces inside it are dropped.
  return [
    ...new Set(
      raw
        .split(/[,;\r\n]+/)
        .map((part) => part.replace(/\s+/g, ""))
        .filter(Boolean)
    )
  ];
}

/*
 * Label PDFs sent along with an outbound, stored in R2.
 *
 * Arrive as data URLs from the page. Anything that is not a PDF is refused
 * rather than stored, because a label that will not print is found out at
 * the moment the parcel has to go.
 */
/*
 * A label that arrives as a photo or screenshot, as a one-page PDF.
 *
 * Carriers and partners send JPEG and PNG labels as often as PDFs, and
 * everything after this - R2, Pack & Ship, printing - expects a PDF. The page
 * is the image's own size, so nothing is scaled.
 */
async function imageLabelToPdf(dataUrl) {
  const match = asText(dataUrl).match(/^data:image\/(jpeg|jpg|png);base64,(.+)$/i);
  if (!match) return null;

  const bytes = Buffer.from(match[2], "base64");
  const pdf = await PDFDocument.create();
  const image = /png/i.test(match[1]) ? await pdf.embedPng(bytes) : await pdf.embedJpg(bytes);
  const page = pdf.addPage([image.width, image.height]);

  page.drawImage(image, { x: 0, y: 0, width: image.width, height: image.height });

  return Buffer.from(await pdf.save());
}

async function storeLabelFiles(files, folder) {
  const stored = [];

  for (const [index, file] of (Array.isArray(files) ? files : []).entries()) {
    let buffer = null;

    try {
      buffer = (await imageLabelToPdf(file?.data_url)) || pdfBufferFromDataUrl(asText(file?.data_url));
    } catch {
      buffer = null;
    }

    if (!buffer || buffer.length < 100 || buffer.subarray(0, 5).toString("latin1") !== "%PDF-") {
      throw Object.assign(new Error(`Label ${index + 1} is not a PDF, JPEG or PNG file`), { statusCode: 400 });
    }

    // A converted image keeps its name with .pdf on the end.
    const name = sanitizeFileName(
      (asText(file?.filename) || `label-${index + 1}.pdf`).replace(/\.(jpe?g|png)$/i, ".pdf")
    );
    const key = `${folder}/${Date.now()}-${index + 1}-${name}`;
    const url = await uploadPdfToR2({ key, pdfBuffer: buffer });

    stored.push({
      url,
      filename: name,
      tracking: asText(file?.tracking) || null,
      uploaded_at: new Date().toISOString()
    });
  }

  return stored;
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

/*
 * Every way a scanned barcode can be stored.
 *
 * A UPC-A is an EAN-13 with a leading zero: 197600040076 and 0197600040076
 * are the same box, and a scanner hands back whichever length the symbol
 * encodes. Looking up only the scanned form found half the stock. Anything
 * that is not a plain barcode is matched exactly as scanned.
 */
function barcodeForms(value) {
  const code = asText(value).replace(/\s+/g, "");

  if (!/^\d{8,14}$/.test(code)) return code ? [code] : [];

  const forms = new Set([code]);
  const stripped = code.replace(/^0+/, "");

  if (code.length > 8 && stripped) {
    for (const length of [12, 13, 14]) {
      if (stripped.length <= length) forms.add(stripped.padStart(length, "0"));
    }
  }

  return [...forms];
}

function barcodeFormula(fieldNames, code) {
  const checks = [];

  for (const field of fieldNames) {
    for (const form of barcodeForms(code)) {
      checks.push(`TRIM({${field}} & '') = '${escapeFormulaValue(form)}'`);
    }
  }

  return checks.length ? `OR(${checks.join(", ")})` : "FALSE()";
}

/*
 * Stock Levels has the barcode in two fields: Product GTIN, and Product EAN
 * for the European boxes. Only the first was ever read, which is why most
 * EAN-13 scans found nothing.
 */
async function findStockLevelByGTIN(gtin) {
  const records = await airtable(AIRTABLE_STOCK_LEVELS_TABLE)
    .select({
      filterByFormula: `AND(
        ${barcodeFormula(["Product GTIN", "Product EAN"], gtin)},
        TRIM({SKU} & '') != '',
        TRIM({Size} & '') != ''
      )`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function findIncomingStockByGTIN(gtin) {
  const records = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
    .select({
      filterByFormula: `AND(
        ${barcodeFormula(["Product GTIN"], gtin)},
        TRIM({SKU} & '') != '',
        TRIM({Size} & '') != ''
      )`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

/*
 * The portal knows what the WMS does not: bol_barcodes, SKU Master and the
 * StockX catalog. Same secret as the Lojiq bot call below.
 */
async function callPortal(pathName, body, { timeoutMs = 20000 } = {}) {
  const secret = process.env.COUNTER_OFFERS_SECRET;

  if (!secret) {
    throw new Error("COUNTER_OFFERS_SECRET is not set on the WMS");
  }

  const response = await fetch(`${KICKZ_PORTAL_BASE_URL.replace(/\/$/, "")}${pathName}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-kc-secret": secret },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs)
  });

  const data = await response.json().catch(() => ({}));

  // A refusal with reasons (a barcode lookup's `reason`, a partner-stock
  // call's `errors`) is an answer for the caller, not a failure.
  if (!response.ok && !data?.reason && !Array.isArray(data?.errors)) {
    throw new Error(`Portal ${pathName} failed: ${data.details || data.error || response.status}`);
  }

  return { ...data, httpStatus: response.status };
}

/*
 * Partner pairs a forward can take, merged into a Create Outbound lookup.
 *
 * A partner's pairs live in Supabase until they leave, so the Ready to
 * Forward units in Airtable are no longer the whole shelf. The pairs travel
 * through the page as "ps:<id>" next to real record ids, and only become
 * Inventory Units when the outbound is submitted. Units that were already in
 * Airtable before the switch still come along as they always did.
 */
const PARTNER_PAIR_PREFIX = "ps:";

async function partnerForwardableLookup({ sellerRecordId, barcode = "", sku = "", size = "" }) {
  const data = await callPortal(
    "/api/internal/partner-stock/forwardable",
    { seller_record_id: sellerRecordId, barcode, sku, size },
    { timeoutMs: 15000 }
  );

  if (!data?.ok) {
    throw new Error((data?.errors || []).join("; ") || "Partner stock lookup failed");
  }

  return data.is_partner ? data : null;
}

function mergePartnerPairs({ partner, records, fallback }) {
  const pairs = partner?.pairs || [];

  if (!pairs.length) return null;

  const first = records[0]?.fields || {};
  const fee = Number(partner.forwarding_fee) || 0;
  const available = records.length + pairs.length;

  return {
    found: true,
    gtin: fallback.gtin || asText(first["Product GTIN"]) || asText(pairs[0].barcode),
    product_name: asText(first["Product Name"]) || asText(pairs[0].product_name),
    sku: asText(first["SKU"]) || pairs[0].sku,
    size: asText(first["Size"]) || pairs[0].size,
    available_quantity: available,
    unit_price: fee,
    total_available_price: fee * available,
    inventory_unit_ids: [
      ...records.map((record) => record.id),
      ...pairs.map((pair) => `${PARTNER_PAIR_PREFIX}${pair.id}`)
    ],
    seller_ids: [fallback.sellerId],
    unit_forwarding_fee: fee
  };
}

const MANUAL_STOCK_SELLER_CODES = [
  "SE-00309",
  "SE-00537",
  "SE-00781",
  "SE-00455"
];

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
    const allowedSellerCodes = ["SE-00537", "SE-00309", "SE-00781", "SE-00455"];

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
    const allowedSellerCodes = ["SE-00537", "SE-00309", "SE-00781", "SE-00455"];

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

  /*
    Partner forwards live in Supabase. Same rule as the Airtable logs: Ready
    to Ship and at least one tracking number. A portal that does not answer
    must not take the rest of Pack & Ship down with it.
  */
  let supabaseForwardOptions = [];

  try {
    const data = await callPortal("/api/internal/forwarding/list", { statuses: ["ready_to_ship"] });

    supabaseForwardOptions = (data?.forwards || [])
      .filter((forward) => (forward.tracking_numbers || []).length > 0)
      .map((forward) => ({
        id: forward.id,
        source_table: "forwarding_log",
        label: `${forward.display_id} - ${forward.buyer_name || forward.seller_name || forward.seller_id}`,
        shipping_status: "Ready to Ship",
        tracking_numbers_count: forward.tracking_numbers.length
      }));
  } catch (error) {
    console.error("pack-ship: partner forwards not loaded:", error.message);
  }

  forwardingOptions.push(...supabaseForwardOptions);

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

function looksLikeAirtableRecordId(value) {
  return /^rec[a-zA-Z0-9]{14}$/.test(asText(value));
}

async function findSellerRecordBySellerId(sellerIdValue) {
  const safeSellerId = escapeFormulaValue(sellerIdValue);

  const records = await airtable(AIRTABLE_SELLERS_TABLE)
    .select({
      fields: ["Seller ID", "Country Code", "Full Name", "Company Name"],
      filterByFormula: `TRIM({Seller ID} & '') = '${safeSellerId}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function getSellerRecordFromLinkedSellerValue(value) {
  const sellerValue = asText(value);

  if (!sellerValue) {
    return null;
  }

  // Case 1 → already Airtable record ID
  if (looksLikeAirtableRecordId(sellerValue)) {
    return await airtable(AIRTABLE_SELLERS_TABLE).find(sellerValue);
  }

  // Case 2 → Seller ID like SE-00001
  return await findSellerRecordBySellerId(sellerValue);
}

async function getSellerCountryCodeFromOrderFields(orderFields) {
  const warehouseSellerCodes = [
    "SE-00309",
    "SE-00537",
    "SE-00781",
    "SE-00455"
  ];

  const linkedSellerValues = Array.isArray(orderFields["Linked Seller ID"])
    ? orderFields["Linked Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  if (linkedSellerValues.length) {
    const linkedValue = asText(linkedSellerValues[0]).trim().toUpperCase();

    if (warehouseSellerCodes.includes(linkedValue)) {
      return "NL";
    }

    const sellerRecord = await getSellerRecordFromLinkedSellerValue(linkedSellerValues[0]);

    if (sellerRecord) {
      const sellerId = asText(sellerRecord.fields["Seller ID"]).trim().toUpperCase();

      if (warehouseSellerCodes.includes(sellerId)) {
        return "NL";
      }

      return asText(sellerRecord.fields["Country Code"]);
    }
  }

  const claimedSellerRecordIds = Array.isArray(orderFields["Claimed Seller ID"])
    ? orderFields["Claimed Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  if (!claimedSellerRecordIds.length) {
    return "";
  }

  const sellerRecord = await airtable(AIRTABLE_SELLERS_TABLE).find(
    claimedSellerRecordIds[0]
  );

  const sellerId = asText(sellerRecord.fields["Seller ID"]).trim().toUpperCase();

  if (warehouseSellerCodes.includes(sellerId)) {
    return "NL";
  }

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

// A buyer is recognised by the business, not by its owner: "Company (Owner)",
// or just the name for a private buyer.
function buyerLabel(fields) {
  const company = asText(fields["Company Name"]);
  const person = asText(fields["Full Name"]);
  if (company && person && company.toLowerCase() !== person.toLowerCase()) return `${company} (${person})`;
  return company || person;
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
      label: buyerLabel(record.fields),
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
    .filter((option) => option.label)
    .sort((a, b) => a.label.localeCompare(b.label, "en", { sensitivity: "base" }));
}

function manualSellerMatchesOrder(orderFields, sellerId, sellerRecordId) {
  const normalizedSellerId = asText(sellerId).toUpperCase();
  const normalizedSellerRecordId = asText(sellerRecordId);

  const linkedSellerValues = Array.isArray(orderFields["Linked Seller ID"])
    ? orderFields["Linked Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  if (linkedSellerValues.length) {
    return linkedSellerValues.some((value) => {
      const normalizedValue = asText(value).toUpperCase();
      return normalizedValue === normalizedSellerId || value === normalizedSellerRecordId;
    });
  }

  const claimedSellerValues = Array.isArray(orderFields["Claimed Seller ID"])
    ? orderFields["Claimed Seller ID"].map((value) => asText(value)).filter(Boolean)
    : [];

  return claimedSellerValues.some((value) => {
    const normalizedValue = asText(value).toUpperCase();
    return normalizedValue === normalizedSellerId || value === normalizedSellerRecordId;
  });
}

async function findManualStockOrderMatch({ sellerId, sellerRecordId, sku, size }) {
  const safeSku = escapeFormulaValue(sku);
  const safeSize = escapeFormulaValue(size);

  const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
    .select({
      fields: [
        "Order ID",
        "Order Date",
        "Fulfillment Status",
        "Client",
        "Shopify Order Number",
        "Store Name",
        "Product Name",
        "SKU",
        "Size",
        "Linked Inventory Unit",
        "Linked Seller ID",
        "Claimed Seller ID",
        "Shipping Label",
        "Tracking Number"
      ],
      filterByFormula: `AND(
        ARRAYJOIN({Linked Inventory Unit}) != '',
        OR(
          {Fulfillment Status} = 'Allocated',
          {Fulfillment Status} = 'Awaiting Label'
        ),
        TRIM({SKU} & '') = '${safeSku}',
        TRIM({Size} & '') = '${safeSize}'
      )`,
      sort: [{ field: "Order Date", direction: "asc" }]
    })
    .all();

  return records.find((record) =>
    manualSellerMatchesOrder(record.fields || {}, sellerId, sellerRecordId)
  ) || null;
}

async function sendLabelRequestForUnfulfilledOrder(orderRecord) {
  const orderFields = orderRecord.fields || {};
  const orderId = asText(orderFields["Order ID"]) || orderRecord.id;

  const clientId = first(orderFields["Client"]);
  if (!clientId) {
    throw new Error(`The Order ${orderId} has no linked Client`);
  }

  const merchantRecord = await airtable(AIRTABLE_MERCHANTS_TABLE).find(clientId);
  const merchantFields = merchantRecord.fields || {};

  const labelRequestChannelId = asText(merchantFields["Label Request Channel ID"]);
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

  return {
    order_id: orderId,
    shopify_order_number: asText(orderFields["Shopify Order Number"]),
    sku: asText(orderFields["SKU"]),
    size: asText(orderFields["Size"])
  };
}

async function markUnfulfilledOrderLabelError(recordId, errorMessage) {
  await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(recordId, {
    "Fulfillment Status": "Label Error",
    "Label Error Message": asText(errorMessage)
  });
}

/*
 * A label for a Lojiq store consignor goes through the Lojiq bot.
 *
 * A seller linked to a Merchant is a Lojiq store. Its labels channel lives in
 * the Lojiq server, where this service's own bot has no access (ORD-024891:
 * 403 Missing Access), and its footer said Kickz Caviar to a store that must
 * never see that name. The Lojiq bot can post there, and the message says
 * Lojiq. Needs COUNTER_OFFERS_SECRET on this service, the same value the
 * portal and that bot use.
 */
const LOJIQ_BOT_URL = process.env.LOJIQ_BOT_URL || "https://airtable-discord-updates.onrender.com";

function isStoreSellerRecord(sellerRecord) {
  const merchants = sellerRecord?.fields?.["Merchants"];

  return Array.isArray(merchants) && merchants.length > 0;
}

async function sendLabelToLojiqStoreChannel({ channelId, orderId, trackingNumber, labelUrl, productName, sku, size }) {
  const secret = process.env.COUNTER_OFFERS_SECRET;

  if (!secret) {
    throw new Error("COUNTER_OFFERS_SECRET is not set on the WMS, so a store label cannot be posted through the Lojiq bot");
  }

  if (!asText(channelId)) {
    throw new Error(`Store consignor has no Labels Channel ID for ${orderId}`);
  }

  const response = await fetch(`${LOJIQ_BOT_URL.replace(/\/$/, "")}/post-member-wtb-store-message`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-kc-secret": secret },
    body: JSON.stringify({
      channel_id: channelId,
      content: "",
      embeds: [
        {
          title: "📦 Shipping Label Ready",
          color: 0x2F80ED,
          description:
            `**Product:** ${productName || "-"}\n` +
            `**SKU:** ${sku || "-"}\n` +
            `**Size:** ${size || "-"}\n\n` +
            `**Order:** ${orderId}\n` +
            `**Tracking:** ${trackingNumber || "-"}\n\n` +
            `[📄 Download Label](${labelUrl})`,
          footer: { text: "Lojiq" }
        }
      ],
      components: []
    })
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(`Lojiq bot refused the label for ${orderId}: ${data.error || response.status}`);
  }

  return true;
}

async function sendFinalLabelToDiscordChannel({
  channelId,
  orderId,
  trackingNumber,
  labelUrl,
  productName,
  sku,
  size,
  markLabelOk = true
}) {
  if (!process.env.DISCORD_TOKEN) {
    throw new Error("Missing DISCORD_TOKEN");
  }

  const fallbackChannelId = "1506989427183058996";
  const targetChannelId = asText(channelId) || fallbackChannelId;

  const url = `https://discord.com/api/v10/channels/${targetChannelId}/messages`;

  const body = {
    embeds: [
      {
        title: "📦 Shipping Label Ready",
        color: 0x00b894,
        description:
          `**Product:** ${productName || "-"}\n` +
          `**SKU:** ${sku || "-"}\n` +
          `**Size:** ${size || "-"}\n\n` +
          `**Order:** ${orderId}\n` +
          `**Tracking:** ${trackingNumber}\n\n` +
          `[📄 Download Label](${labelUrl})`,
        footer: {
          text: asText(channelId)
            ? "Kickz Caviar"
            : "Kickz Caviar • Internal fallback"
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

  if (markLabelOk && asText(channelId)) {
    await markChannelLabelOk(channelId);
  }

  return true;
}

async function sendFinalLabelToDiscordDM({
  discordUserId,
  orderId,
  trackingNumber,
  labelUrl,
  productName,
  sku,
  size
}) {
  if (!process.env.DISCORD_TOKEN) {
    throw new Error("Missing DISCORD_TOKEN");
  }

  const userId = asText(discordUserId);

  if (!userId) {
    throw new Error("Missing Discord ID for seller DM fallback");
  }

  const dmResponse = await fetch("https://discord.com/api/v10/users/@me/channels", {
    method: "POST",
    headers: {
      "Authorization": `Bot ${process.env.DISCORD_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      recipient_id: userId
    })
  });

  const dmText = await dmResponse.text().catch(() => "");
  let dmData = {};

  try {
    dmData = dmText ? JSON.parse(dmText) : {};
  } catch {
    dmData = {};
  }

  if (!dmResponse.ok || !dmData.id) {
    throw new Error(`Failed to create DM: ${dmResponse.status} ${dmText}`);
  }

  const messageResponse = await fetch(`https://discord.com/api/v10/channels/${dmData.id}/messages`, {
    method: "POST",
    headers: {
      "Authorization": `Bot ${process.env.DISCORD_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      embeds: [
        {
          title: "📦 Shipping Label Ready",
          color: 0x00b894,
          description:
            `**Product:** ${productName || "-"}\n` +
            `**SKU:** ${sku || "-"}\n` +
            `**Size:** ${size || "-"}\n\n` +
            `**Order:** ${orderId}\n` +
            `**Tracking:** ${trackingNumber}\n\n` +
            `[📄 Download Label](${labelUrl})`,
          footer: {
            text: "Kickz Caviar"
          }
        }
      ]
    })
  });

  const messageText = await messageResponse.text().catch(() => "");

  if (!messageResponse.ok) {
    throw new Error(`Discord DM send failed: ${messageResponse.status} ${messageText}`);
  }

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
/*
 * The label-request endpoints below serve two kinds of order.
 *
 * A store order lives in this base and is handled here, exactly as it
 * always was. A Lojiq manual order is a Member WTB and lives in the
 * Kickz Caviar portal, which already owns every rule about it: where the
 * file is stored, which fields it writes, what happens after. So these
 * two forward and normalise, and hold no rule of their own - a second
 * copy of those rules is precisely what would drift apart.
 */
/*
 * Whether this id is really a row in Unfulfilled Orders Log.
 *
 * .find() cannot answer that: an Airtable record id resolves across the
 * whole base, so asking this table for a Member WTB id hands one back,
 * and the fields the two tables happen to share then fill in while the
 * rest sits empty. That is not an error anywhere - it just quietly shows
 * half a page. A select scoped to the table only returns rows that are
 * actually in it, so this is the question the id can honestly answer.
 *
 * It exists so the two endpoints below do not depend on the caller
 * passing a type. The type is still read first and still decides when it
 * is there; this is what happens when a link predates it.
 */
async function isUnfulfilledOrderRecord(recordId) {
  const records = await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE)
    .select({
      filterByFormula: `RECORD_ID() = "${recordId}"`,
      maxRecords: 1
    })
    .firstPage();

  return records.length > 0;
}

/*
 * Which of the two kinds of order an id is, given whatever the caller
 * said about it. An explicit type wins and costs nothing; without one
 * the base is asked.
 */
async function resolveLabelRequestType(recordId, declaredType) {
  if (asText(declaredType) === "member_wtb") return "member_wtb";

  return (await isUnfulfilledOrderRecord(recordId))
    ? "store_order"
    : "member_wtb";
}

async function fetchKickzMemberWtbLabelRequest(recordId) {
  const url =
    `${KICKZ_PORTAL_BASE_URL}/api/member-wtb/label-request/${encodeURIComponent(recordId)}`;

  const response = await fetch(url);
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(
      data.details || data.error || `Label request lookup failed (${response.status})`
    );
  }

  // Mapped onto the shape a store order already answers with, so the page
  // keeps one renderer instead of a branch per field.
  return {
    record_id: asText(data.record_id) || recordId,
    order_id: asText(data.member_wtb_id),
    shopify_order_number: "",
    product_name: asText(data.product_name),
    size: asText(data.size),
    sku: asText(data.sku),
    store_name: asText(data.buyer_name),
    fulfillment_status: "",
    tracking_number: asText(data.tracking_number)
  };
}

async function submitKickzMemberWtbLabel({
  recordId,
  trackingNumber,
  fileName,
  fileDataUrl,
  fileType
}) {
  const response = await fetch(
    `${KICKZ_PORTAL_BASE_URL}/api/member-wtb/label-request-submit`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        member_wtb_record_id: recordId,
        tracking_number: trackingNumber,
        label_file: {
          name: fileName,
          type: fileType || "application/pdf",
          data: fileDataUrl
        }
      })
    }
  );

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(
      data.details || data.error || `Label submit failed (${response.status})`
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

async function findIncomingReturnByTrackingNumber(trackingNumber) {
  const safeTrackingNumber = escapeFormulaValue(trackingNumber);

  if (!safeTrackingNumber) {
    return null;
  }

  const records = await airtable(AIRTABLE_RETURNS_TABLE)
    .select({
      filterByFormula: `TRIM({Tracking Number} & '') = '${safeTrackingNumber}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
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

  if (sourceTable === "forwarding_log") {
    const data = await callPortal("/api/internal/forwarding/get", { id: outboundId });

    if (!data?.ok) throw new Error((data?.errors || []).join(" ") || "Forward not found");

    return {
      id: data.forward.id,
      source_table: "forwarding_log",
      shipping_status: data.forward.shipping_status === "ready_to_ship" ? "Ready to Ship" : data.forward.shipping_status,
      tracking_numbers: data.forward.tracking_numbers || [],
      shipping_labels: (data.forward.labels || []).map((label) => ({ url: label.url, filename: label.filename })),
      items: (data.pairs || []).map((pair) => ({
        id: `${PARTNER_PAIR_PREFIX}${pair.id}`,
        gtin: asText(pair.barcode),
        product_name: asText(pair.product_name),
        sku: asText(pair.sku),
        size: asText(pair.size)
      }))
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

app.get("/api/manual-stock-sellers", async (_req, res) => {
  try {
    const records = await Promise.all(
      MANUAL_STOCK_SELLER_CODES.map((sellerCode) =>
        findSellerRecordBySellerId(sellerCode)
      )
    );

    const sellers = records
      .filter(Boolean)
      .map((record) => {
        const sellerId = asText(record.fields["Seller ID"]);
        const companyName = asText(record.fields["Company Name"]);
        const fullName = asText(record.fields["Full Name"]);

        return {
          seller_id: sellerId,
          seller_record_id: record.id,
          label: `${sellerId} — ${companyName || fullName || "Unknown"}`
        };
      });

    return res.status(200).json({
      ok: true,
      sellers
    });
  } catch (error) {
    console.error("manual-stock-sellers failed:", error);
    return res.status(500).json({
      error: "Failed to load manual stock sellers",
      details: error.message
    });
  }
});

app.post("/api/manual-stock-label-request", async (req, res) => {
  try {
    const sellerId = asText(req.body?.seller_id).toUpperCase();
    const sellerRecordId = asText(req.body?.seller_record_id);
    const trackingNumber = asText(req.body?.tracking_number);
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    if (!MANUAL_STOCK_SELLER_CODES.includes(sellerId)) {
      return res.status(400).json({ error: "Invalid seller_id" });
    }

    if (!sellerRecordId) {
      return res.status(400).json({ error: "Missing seller_record_id" });
    }

    if (!items.length) {
      return res.status(400).json({ error: "No items provided" });
    }

    const results = [];

    for (const item of items) {
      const sku = asText(item.sku);
      const size = asText(item.size);

      if (!sku || !size) {
        results.push({
          sku,
          size,
          status: "missing_input"
        });
        continue;
      }

      const orderRecord = await findManualStockOrderMatch({
        sellerId,
        sellerRecordId,
        sku,
        size
      });

      if (!orderRecord) {
        results.push({
          sku,
          size,
          status: "not_found"
        });
        continue;
      }

      const labelResult = await sendLabelRequestForUnfulfilledOrder(orderRecord);

      results.push({
        sku,
        size,
        status: "label_requested",
        order_id: labelResult.order_id,
        shopify_order_number: labelResult.shopify_order_number
      });
    }

    const matchedCount = results.filter((result) => result.status === "label_requested").length;
    const notFoundCount = results.filter((result) => result.status === "not_found").length;

    let incomingStockCreated = false;

    if (matchedCount === 0 && trackingNumber) {
      await airtable(AIRTABLE_INCOMING_STOCK_TABLE).create({
        "Tracking Number": trackingNumber,
        "Status": "Received",
        "Received At": new Date().toISOString()
      });

      incomingStockCreated = true;
    }

    return res.status(200).json({
      ok: true,
      matched_count: matchedCount,
      not_found_count: notFoundCount,
      incoming_stock_created: incomingStockCreated,
      results
    });
  } catch (error) {
    console.error("manual-stock-label-request failed:", error);
    return res.status(500).json({
      error: "Manual stock label request failed",
      details: error.message
    });
  }
});

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

    const isBarcode = /^\d{8,14}$/.test(gtin.replace(/\s+/g, ""));

    /*
     * Cheapest first, all at once: the barcode table in Supabase, then Stock
     * Levels, then earlier intake. Only when none of them knows the box is
     * StockX asked, because that is the one call that takes a second.
     *
     * A failure of the quick portal check is not fatal - Airtable may still
     * know the barcode, and StockX gets its own try below.
     */
    const [catalog, stockLevel, incoming] = await Promise.all([
      isBarcode
        ? callPortal("/api/internal/lookup-barcode", { barcode: gtin, stockx: false }, { timeoutMs: 8000 })
            .catch((err) => {
              console.error("lookup-product: quick portal check failed:", err.message);
              return null;
            })
        : null,
      findStockLevelByGTIN(gtin),
      findIncomingStockByGTIN(gtin)
    ]);

    const answer = (sku, size, source, extra = {}) =>
      res.status(200).json({ found: true, gtin, sku: asText(sku), size: asText(size), source, ...extra });

    if (catalog?.found) {
      return answer(catalog.sku, catalog.size, "bol_barcodes", {
        alternatives: catalog.alternatives || []
      });
    }

    if (stockLevel) {
      return answer(stockLevel.fields?.["SKU"], stockLevel.fields?.["Size"], "stock_levels");
    }

    if (incoming) {
      return answer(incoming.fields?.["SKU"], incoming.fields?.["Size"], "incoming_stock");
    }

    let reason = "not_found";

    if (isBarcode) {
      try {
        const fromStockx = await callPortal("/api/internal/lookup-barcode", { barcode: gtin });

        if (fromStockx.found) {
          return answer(fromStockx.sku, fromStockx.size, fromStockx.source || "stockx", {
            alternatives: fromStockx.alternatives || []
          });
        }

        reason = fromStockx.reason || "not_found";
      } catch (err) {
        console.error("lookup-product: StockX lookup failed:", err.message);
        reason = "lookup_failed";
      }
    }

    return res.status(200).json({
      found: false,
      gtin,
      sku: "",
      size: "",
      source: null,
      // "lookup_failed" means try again; "not_found" means type it in.
      reason: reason === "lookup_failed" ? "lookup_failed" : "not_found"
    });
  } catch (error) {
    console.error("lookup-product failed:", error);
    return res.status(500).json({
      error: "Failed to lookup product",
      details: error.message
    });
  }
});

/*
 * Name and picture for a SKU, so the person scanning sees whether the box in
 * hand is the shoe on screen.
 *
 * Goes through the portal's resolver: SKU Master first, StockX on an exact
 * match only, and a SKU Master record is created for a code that did not
 * have one yet. `learn` is for a SKU typed in by hand - every size of it is
 * then stored with its barcodes, so the next box of that SKU just scans.
 */
app.post("/api/product-info", async (req, res) => {
  const sku = asText(req.body?.sku).toUpperCase();

  if (!sku) {
    return res.status(400).json({ error: "Missing sku" });
  }

  if (req.body?.learn) {
    callPortal("/api/internal/learn-barcodes", { sku }).catch((err) =>
      console.error("learn-barcodes failed:", { sku, error: err.message })
    );
  }

  try {
    const data = await callPortal("/api/internal/resolve-sku", { sku });
    const result = data?.results?.[sku] || { ok: false, reason: "not_found" };

    return res.status(200).json({ sku, ...result });
  } catch (error) {
    console.error("product-info failed:", { sku, error: error.message });

    return res.status(200).json({ sku, ok: false, reason: "lookup_failed" });
  }
});

/*
 * Partners: sellers who keep pairs in our warehouse.
 *
 * Marked by a Default VAT Type on Sellers Database - the field exists only
 * for this, and it is also the VAT type their pairs get at intake.
 */
async function getPartnerOptions() {
  const records = await airtable(AIRTABLE_SELLERS_TABLE)
    .select({
      fields: ["Full Name", "Seller ID", "Default VAT Type", "Forwarding Fee"],
      filterByFormula: `NOT({Default VAT Type} = BLANK())`,
      sort: [{ field: "Full Name", direction: "asc" }]
    })
    .all();

  return records
    .map((record) => ({
      id: record.id,
      label: asText(record.fields["Full Name"]) || asText(record.fields["Seller ID"]),
      seller_id: asText(record.fields["Seller ID"]),
      vat_type: asText(record.fields["Default VAT Type"]),
      forwarding_fee: Number(record.fields["Forwarding Fee"]) || 0
    }))
    .filter((option) => option.seller_id);
}

app.get("/api/partners", async (_req, res) => {
  try {
    return res.status(200).json({ ok: true, partners: await getPartnerOptions() });
  } catch (error) {
    console.error("partners failed:", error);
    return res.status(500).json({ error: "Failed to load partners", details: error.message });
  }
});

// Pass a partner-stock refusal through with its own status and reasons.
function sendPortalAnswer(res, data, fallbackError) {
  if (data?.ok) return res.status(200).json(data);

  return res.status(data?.httpStatus && data.httpStatus >= 400 ? data.httpStatus : 500).json({
    ok: false,
    errors: Array.isArray(data?.errors) && data.errors.length ? data.errors : [fallbackError]
  });
}

const PARTNER_INTAKE_MODES = {
  Consignment: "consignment",
  Forwarding: "forwarding",
  Both: "both"
};

/*
 * A partner parcel: every pair into Supabase partner_stock, nothing into
 * Incoming Stock or Inventory Units. Listed pairs are on consignment within
 * one sync round; a unit only appears when a pair sells or is sent on.
 *
 * The parcel's placeholder row in Incoming Stock, made when the parcel was
 * received, is still marked Verified, so that log keeps showing which parcels
 * were checked. It carries no SKU, so nothing downstream acts on it.
 */
app.post("/api/submit-partner-intake", async (req, res) => {
  try {
    const trackingNumber = asText(req.body?.tracking_number);
    const mode = PARTNER_INTAKE_MODES[asText(req.body?.type)];
    const sellerRecordId = asText(req.body?.seller_record_id);
    const items = Array.isArray(req.body?.items) ? req.body.items : [];

    if (!trackingNumber) return res.status(400).json({ ok: false, errors: ["Missing tracking number"] });
    if (!mode) return res.status(400).json({ ok: false, errors: ["Pick Consignment, Forwarding or both"] });
    if (!sellerRecordId) return res.status(400).json({ ok: false, errors: ["Pick the partner"] });

    const data = await callPortal(
      "/api/internal/partner-stock/intake",
      {
        seller_record_id: sellerRecordId,
        mode,
        tracking_number: trackingNumber,
        append: Boolean(req.body?.append),
        items: items.map((item) => ({
          barcode: asText(item.gtin),
          sku: asText(item.sku),
          size: asText(item.size),
          quantity: Number(item.quantity),
          partner_price: item.partner_price,
          markup: item.markup
        }))
      },
      { timeoutMs: 60000 }
    );

    if (!data?.ok) return sendPortalAnswer(res, data, "Intake failed");

    try {
      const placeholders = await airtable(AIRTABLE_INCOMING_STOCK_TABLE)
        .select({
          filterByFormula: `AND(
            TRIM({Tracking Number} & '') = '${escapeFormulaValue(trackingNumber)}',
            TRIM({SKU} & '') = ''
          )`,
          maxRecords: 1
        })
        .firstPage();

      if (placeholders[0]) {
        await airtable(AIRTABLE_INCOMING_STOCK_TABLE).update(placeholders[0].id, {
          "Status": "Verified",
          "Verified At": new Date().toISOString(),
          "Supplier": [sellerRecordId]
        });
      }
    } catch (logError) {
      // The pairs are in; a parcel log that did not update is not worth
      // failing the intake over.
      console.error("submit-partner-intake: parcel log not updated:", logError.message);
    }

    return res.status(200).json(data);
  } catch (error) {
    console.error("submit-partner-intake failed:", error);
    return res.status(500).json({ ok: false, errors: ["Intake failed"], details: error.message });
  }
});

app.get("/api/partner-stock", async (req, res) => {
  try {
    const statuses = asText(req.query?.statuses || "in_stock")
      .split(",")
      .map((status) => status.trim())
      .filter(Boolean);

    const data = await callPortal("/api/internal/partner-stock/list", {
      seller_record_id: asText(req.query?.seller_record_id),
      statuses,
      sku: asText(req.query?.sku)
    }, { timeoutMs: 60000 });

    return sendPortalAnswer(res, data, "Could not load partner stock");
  } catch (error) {
    console.error("partner-stock list failed:", error);
    return res.status(500).json({ ok: false, errors: ["Could not load partner stock"], details: error.message });
  }
});

app.post("/api/partner-stock/update", async (req, res) => {
  try {
    const data = await callPortal("/api/internal/partner-stock/update", {
      seller_record_id: asText(req.body?.seller_record_id),
      ids: Array.isArray(req.body?.ids) ? req.body.ids : [],
      changes: req.body?.changes || {}
    }, { timeoutMs: 60000 });

    return sendPortalAnswer(res, data, "Update failed");
  } catch (error) {
    console.error("partner-stock update failed:", error);
    return res.status(500).json({ ok: false, errors: ["Update failed"], details: error.message });
  }
});

/*
 * Direct: stock we bought ourselves, straight into Inventory Units.
 *
 * Built, but off until DIRECT_INTAKE_ENABLED=true on this service. When it is
 * on: pick Direct, the seller it was bought from, and a purchase price per
 * line, and every pair becomes an Available unit on submit.
 */
const DIRECT_INTAKE_ENABLED = /^(1|true|yes|on)$/i.test(process.env.DIRECT_INTAKE_ENABLED || "");

app.get("/api/direct-intake-status", (_req, res) => {
  res.json({ enabled: DIRECT_INTAKE_ENABLED });
});

app.post("/api/submit-direct-intake", async (req, res) => {
  if (!DIRECT_INTAKE_ENABLED) {
    return res.status(403).json({ ok: false, errors: ["Direct intake is not switched on yet"] });
  }

  try {
    const trackingNumber = asText(req.body?.tracking_number);
    const sellerRecordId = asText(req.body?.seller_record_id);
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    const errors = [];

    if (!sellerRecordId) errors.push("Pick the seller it was bought from");
    if (!items.length) errors.push("No items");

    const lines = items.map((item) => ({
      gtin: asText(item.gtin),
      sku: asText(item.sku).toUpperCase(),
      size: asText(item.size),
      quantity: Number(item.quantity),
      price: Number(String(item.partner_price ?? "").replace(",", "."))
    }));

    for (const line of lines) {
      const name = `${line.sku || "?"} / ${line.size || "?"}`;
      if (!line.sku || !line.size) errors.push(`${name}: SKU and size are required`);
      if (!Number.isInteger(line.quantity) || line.quantity < 1) errors.push(`${name}: invalid quantity`);
      if (!(line.price > 0)) errors.push(`${name}: purchase price is required`);
    }

    if (errors.length) return res.status(400).json({ ok: false, errors });

    const seller = await airtable(AIRTABLE_SELLERS_TABLE).find(sellerRecordId);
    const vatType = asText(seller.fields["Default VAT Type"]) || "Margin";

    // Names and brands from the catalogue, so the units are not nameless.
    const catalogue = await callPortal("/api/internal/resolve-sku", {
      skus: [...new Set(lines.map((line) => line.sku))].slice(0, 50)
    }).catch(() => ({ results: {} }));
    const today = new Date().toISOString().split("T")[0];

    const fieldsList = lines.flatMap((line) =>
      Array.from({ length: line.quantity }, () => {
        const product = catalogue?.results?.[line.sku] || {};

        const fields = {
          "Product Name": asText(product.product_name),
          "Brand": asText(product.brand),
          "SKU": line.sku,
          "Size": line.size,
          "VAT Type": vatType,
          "Purchase Price": line.price,
          "Payment Note": String(line.price),
          "Purchase Date": today,
          "Seller ID": [sellerRecordId],
          "Ticket Number": trackingNumber,
          "Type": "Direct",
          "Source": "Regular",
          "Verification Status": "Verified",
          "Payment Status": "To Pay",
          "Availability Status": "Available"
        };

        if (line.gtin) fields["Product GTIN"] = line.gtin;

        return { fields };
      })
    );

    let created = 0;

    for (let i = 0; i < fieldsList.length; i += 10) {
      const records = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE).create(fieldsList.slice(i, i + 10));
      created += records.length;
    }

    return res.status(200).json({ ok: true, units_created: created });
  } catch (error) {
    console.error("submit-direct-intake failed:", error);
    return res.status(500).json({ ok: false, errors: ["Direct intake failed"], details: error.message });
  }
});

/*
 * Store one label PDF and hand back its address.
 *
 * For the Lojiq Admin portal, which adds labels to a forward after the fact
 * and has no R2 keys of its own.
 */
app.post("/api/upload-label-file", async (req, res) => {
  try {
    const [stored] = await storeLabelFiles(
      [{ filename: req.body?.file_name, data_url: req.body?.file_data_url, tracking: req.body?.tracking }],
      asText(req.body?.folder).replace(/[^a-z0-9-]/gi, "") || "labels"
    );

    return res.status(200).json({ ok: true, ...stored });
  } catch (error) {
    console.error("upload-label-file failed:", error.message);
    return res.status(error.statusCode || 500).json({ ok: false, error: error.message });
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

    // 0. First check Incoming Returns
    const incomingReturnRecord = await findIncomingReturnByTrackingNumber(trackingNumber);
    
    if (incomingReturnRecord) {
      const currentStatus = asText(incomingReturnRecord.fields["Return Status"]);
    
      if (currentStatus === "Received") {
        return res.status(200).json({
          message: "Return already received",
          exists: true,
          flow_type: "return",
          already_received: true,
          return_record_id: incomingReturnRecord.id
        });
      }
    
      await airtable(AIRTABLE_RETURNS_TABLE).update(incomingReturnRecord.id, {
        "Return Status": "Received",
        "Received At": now
      });
    
      return res.status(200).json({
        message: "Return received",
        exists: true,
        flow_type: "return",
        already_received: false,
        return_record_id: incomingReturnRecord.id
      });
    }

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
          "Tracking Number",
          "Linked Seller ID",
          "Claimed Seller ID"
        ],
        filterByFormula: `OR(
          TRIM({StockX Tracking Number} & '') = '${safeTracking}',
          TRIM({GOAT Tracking Number} & '') = '${safeTracking}'
        )`
      })
      .all();

    if (unfulfilledRecords.length > 0) {
      const nonAllowedStatusRecord = unfulfilledRecords.find((record) => {
        const fulfillmentStatus = asText(record.fields["Fulfillment Status"]);
        return !["Awaiting Label", "Allocated"].includes(fulfillmentStatus);
      });

      if (nonAllowedStatusRecord) {
        const orderId = asText(nonAllowedStatusRecord.fields["Order ID"]) || nonAllowedStatusRecord.id;
        const fulfillmentStatus = asText(nonAllowedStatusRecord.fields["Fulfillment Status"]);
      
        return res.status(400).json({
          error: `The Order ${orderId} has Fulfillment Status "${fulfillmentStatus}" and cannot request a label through this scan flow`
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

    // 3. Fallback: no direct Incoming Stock yet.
    // Let the user try Manual Stock Input first.
    return res.status(200).json({
      message: `No order found for tracking ${trackingNumber}. Use Manual Stock Input below.`,
      exists: false,
      no_match: true,
      tracking_number: trackingNumber
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

    const orderType = await resolveLabelRequestType(
      recordId,
      req.query?.type
    );

    if (orderType === "member_wtb") {
      const order = await fetchKickzMemberWtbLabelRequest(recordId);

      // Answered rather than assumed, so the page can lay itself out
      // for the right kind of order even when the link said nothing.
      return res.status(200).json({
        ok: true,
        order_type: orderType,
        order
      });
    }

    const record = await getUnfulfilledOrderRecordById(recordId);
    const fields = record.fields || {};

    return res.status(200).json({
      ok: true,
      order_type: orderType,
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
  // Read by the catch below, which must not write onto a record that
  // is not in this table. Set before anything can throw.
  let isMemberWtbRequest = asText(req.body?.type) === "member_wtb";

  try {
    const recordId = asText(req.body?.record_id);
    const trackingNumber = asText(req.body?.tracking_number);
    const fileName = asText(req.body?.file_name);
    const fileDataUrl = asText(req.body?.file_data_url);

    if (!recordId) {
      return res.status(400).json({ error: "Missing record_id" });
    }

    /*
     * Required, except where nobody has one to give.
     *
     * Every flow that reaches here through a store or a consignor produces
     * the label itself, so a missing tracking number means a caller made a
     * mistake and should be told - that guard stays exactly as it was.
     *
     * A marketplace label is the exception. Woovin books the parcel on its
     * own carrier account and exposes no tracking number anywhere: not in
     * the order, not in the webhook, not in their dashboard. We read what
     * we can off the label itself, and when the carrier draws the number
     * rather than writing it there is nothing to read. Refusing the label
     * over that would leave a consignor unable to ship at all, which is a
     * far worse outcome than a blank column.
     */
    const isMarketplaceLabel = asText(req.body?.type) === "marketplace";

    if (!trackingNumber && !isMarketplaceLabel) {
      return res.status(400).json({ error: "Missing tracking_number" });
    }

    if (!fileName) {
      return res.status(400).json({ error: "Missing file_name" });
    }

    if (!fileDataUrl) {
      return res.status(400).json({ error: "Missing file_data_url" });
    }

    const orderType = await resolveLabelRequestType(
      recordId,
      req.body?.type
    );

    isMemberWtbRequest = orderType === "member_wtb";

    if (isMemberWtbRequest) {
      await submitKickzMemberWtbLabel({
        recordId,
        trackingNumber,
        fileName,
        fileDataUrl,
        fileType: asText(req.body?.file_type)
      });

      // Kickz Caviar writes the label, the tracking number and the
      // timestamp; the automation on that timestamp then posts the label
      // to Discord. Nothing is written from here.
      return res.status(200).json({
        ok: true,
        message: "Label saved"
      });
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

    // A Member WTB id is not a row in Unfulfilled Orders Log, and an
    // Airtable record id resolves across the whole base - so without this
    // guard a failed manual upload would write its error onto whatever
    // that id happens to be.
    if (recordId && !isMemberWtbRequest) {
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

    if (sourceTable === "forwarding_log") {
      const data = await callPortal("/api/internal/forwarding/ship", {
        id: outboundId,
        items_per_parcel: itemsPerParcel
      });

      if (!data?.ok) {
        return res.status(409).json({ error: (data?.errors || []).join(" ") || "Could not ship this forward" });
      }

      return res.status(200).json({ ok: true });
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
        label: buyerLabel(created.fields),
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

    const partnerForGtin = mode === "Forwarding"
      ? await partnerForwardableLookup({ sellerRecordId: sellerId, barcode: gtin })
      : null;

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

    const withPartnerPairsForGtin = mergePartnerPairs({
      partner: partnerForGtin,
      records,
      fallback: { gtin, sellerId }
    });

    if (withPartnerPairsForGtin) {
      return res.status(200).json(withPartnerPairsForGtin);
    }

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

    const partnerForSku = mode === "Forwarding"
      ? await partnerForwardableLookup({ sellerRecordId: sellerId, sku, size })
      : null;

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

    const withPartnerPairsForSku = mergePartnerPairs({
      partner: partnerForSku,
      records,
      fallback: { gtin: "", sellerId }
    });

    if (withPartnerPairsForSku) {
      return res.status(200).json(withPartnerPairsForSku);
    }

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
    const trackingNumbers = trackingList(req.body?.tracking_numbers);
    const labelFiles = Array.isArray(req.body?.label_files) ? req.body.label_files : [];

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

    const partnerPairIds = linkedInventoryUnitIds
      .filter((id) => String(id).startsWith(PARTNER_PAIR_PREFIX))
      .map((id) => String(id).slice(PARTNER_PAIR_PREFIX.length));

    const airtableUnitIds = linkedInventoryUnitIds.filter(
      (id) => !String(id).startsWith(PARTNER_PAIR_PREFIX)
    );

    // Partner pairs are only ever offered for forwarding; selling them from
    // here would bypass the partner's price entirely.
    if (mode === "Selling" && partnerPairIds.length) {
      return res.status(400).json({ error: "Partner pairs can only be forwarded from here" });
    }

    // A forward is either partner pairs (Supabase) or old Airtable units, not
    // both: they end up in two different logs.
    if (partnerPairIds.length && airtableUnitIds.length) {
      return res.status(400).json({
        error: "Partner pairs and old Airtable units cannot go in one outbound. Submit them separately."
      });
    }

    // Tracking typed in now makes the outbound Ready to Ship straight away.
    const shippingFieldsFor = async (folder) => {
      const stored = await storeLabelFiles(labelFiles, folder);
      const fields = {};

      if (trackingNumbers.length) {
        fields["Tracking Numbers"] = trackingNumbers.join(", ");
      }

      if (stored.length) {
        fields["Shipping Labels"] = stored.map((file) => ({ url: file.url, filename: file.filename }));
      }

      // A label alone is something to ship with too - the same rule as the
      // admin's Forward Service and External Sales.
      if (trackingNumbers.length || stored.length) {
        fields["Shipping Status"] = "Ready to Ship";
      }

      return { fields, stored };
    };

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
      
      const { fields: salesShippingFields } = await shippingFieldsFor("external-sales");

      const createdRecord = await airtable(AIRTABLE_EXTERNAL_SALES_LOG_TABLE).create({
        "Buyer ID": [mainBuyerRecord.id],
        "Linked Inventory Units": linkedInventoryUnitIds,
        "Total Selling Price": totalSellingPrice,
        "Shipping Costs": shippingCosts,
        "Amount of Labels": shippingLabels,
        "Sale Date": new Date().toISOString().split("T")[0],
        ...salesShippingFields
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

    let mainBuyerRecord = null;
    let buyerIdValue = "";

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

      buyerIdValue = asText(externalBuyer.fields["Buyer ID"]);
      mainBuyerRecord = await findMainBuyerRecordByBuyerId(buyerIdValue);

      if (!mainBuyerRecord) {
        return res.status(400).json({
          error: `No matching buyer found in main Airtable for Buyer ID ${buyerIdValue}`
        });
      }
    }

    /*
      Partner pairs: one forward in Supabase, no Inventory Units and no row in
      the Airtable Forwarding Service Log. The portal takes the pairs off the
      shelf and writes the forward in one go, or refuses and changes nothing.
    */
    if (partnerPairIds.length) {
      const stored = await storeLabelFiles(labelFiles, "forwarding");
      const buyerDetails = mainBuyerRecord ? await describeMainBuyer(mainBuyerRecord.id) : null;

      const data = await callPortal("/api/internal/forwarding/create", {
        seller_record_id: sellerId,
        pair_ids: partnerPairIds,
        shipping_costs: shippingCosts,
        labels_needed: shippingLabels,
        tracking_numbers: trackingNumbers,
        labels: stored,
        buyer: mainBuyerRecord
          ? {
              record_id: mainBuyerRecord.id,
              buyer_id: buyerIdValue,
              name: buyerDetails?.name || "",
              country: buyerDetails?.country || ""
            }
          : null
      }, { timeoutMs: 60000 });

      if (!data?.ok) {
        return res.status(data?.httpStatus === 409 ? 409 : 400).json({
          error: (data?.errors || []).join(" ") || "The forward could not be created"
        });
      }

      return res.status(200).json({
        ok: true,
        id: data.forward.id,
        forwarding_id: data.forward.display_id,
        partner_pairs_forwarded: data.pairs.length,
        shipping_status: data.forward.shipping_status
      });
    }

    // Old Airtable units: the Forwarding Service Log, as before.
    const forwardingUnitFees = items
      .map((item) => Number(item?.unit_forwarding_fee))
      .filter((value) => Number.isFinite(value));

    const averageForwardingFee = forwardingUnitFees.length
      ? forwardingUnitFees.reduce((sum, value) => sum + value, 0) / forwardingUnitFees.length
      : 0;

    const { fields: forwardingShippingFields } = await shippingFieldsFor("forwarding");

    const createFields = {
      "Seller ID": [sellerId],
      "Linked Inventory Units": airtableUnitIds,
      "Shipping Costs": shippingCosts,
      "Amount of Labels": shippingLabels,
      "Unit Forwarding Fee": averageForwardingFee,
      "Forwarding Date": new Date().toISOString().split("T")[0],
      ...forwardingShippingFields
    };

    if (mainBuyerRecord) createFields["Buyer ID"] = [mainBuyerRecord.id];

    const createdRecord = await airtable(AIRTABLE_FORWARDING_SERVICE_LOG_TABLE).create(createFields);

    await updateInventoryUnitsToForwardPending(airtableUnitIds);

    return res.status(200).json({
      ok: true,
      id: createdRecord.id,
      linked_inventory_units_count: airtableUnitIds.length
    });
  } catch (error) {
    console.error("submit-outbound failed:", error);
    return res.status(error.statusCode || 500).json({
      error: error.statusCode ? error.message : "Failed to submit outbound",
      details: error.message
    });
  }
});

/*
 * Name and country of a main-base buyer, kept on the forward so the Admin
 * portal can show it without a round trip to Airtable.
 */
async function describeMainBuyer(recordId) {
  try {
    const record = await airtable(AIRTABLE_BUYERS_TABLE).find(recordId);
    const f = record.fields || {};
    const pick = (...names) => {
      for (const name of names) {
        const value = Array.isArray(f[name]) ? f[name][0] : f[name];
        if (asText(value)) return asText(value);
      }
      return "";
    };

    return {
      name: pick("Full Name", "Company Name", "Buyer Name", "Name"),
      country: pick("Country", "Buyer Country")
    };
  } catch (error) {
    console.error("describeMainBuyer failed:", recordId, error.message);
    return null;
  }
}

/*
 * A label for a marketplace sale, made by us.
 *
 * The three marketplaces differ in exactly one thing: who draws the label.
 * SneakerAsk hands us one, Woovin books it on their own account, and bol
 * does neither - their order is addressed to a private shopper and the
 * parcel is ours to ship. So this is the only one of the three where we go
 * to Sendcloud, and it is why it lives here rather than in the consignment
 * service: the keys, the R2 bucket and the Discord delivery are all here
 * already.
 *
 * The consignor never sees an address. He gets a finished label in his own
 * channel, exactly as he does for a store order.
 */
/*
 * The carrier contracts on the Sendcloud account, for choosing one by hand.
 *
 * Read-only and only ever shown on a dry run: which contract a label is paid
 * from is a decision, and seeing the ids is how it gets made.
 */
async function listSendcloudContracts(carrierCode) {
  const res = await fetch("https://panel.sendcloud.sc/api/v2/contracts", {
    headers: { Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY) }
  });

  const body = await res.json().catch(() => ({}));

  if (!res.ok) return { error: `${res.status} ${JSON.stringify(body).slice(0, 300)}` };

  return (body.contracts || body.data || [])
    .filter((c) => !carrierCode || asText(c?.carrier?.code || c?.carrier).toLowerCase() === carrierCode.toLowerCase())
    .map((c) => ({
      id: c.id,
      carrier: c?.carrier?.code || c?.carrier || null,
      name: c.name || c.client_id || null,
      is_active: c.is_active ?? null,
      is_default: c.is_default ?? null,
      type: c.type || (c.is_sendcloud ? "sendcloud" : null)
    }));
}

async function createMarketplaceLabel({ orderRecord, orderFields, orderId, dry = false, contractId = null }) {
  const customerAddress = customerAddressFromOrderFields(orderFields);

  if (!customerAddress.country) {
    throw new Error(`Order ${orderId} has no customer country to ship to`);
  }

  if (!customerAddress.houseNumber) {
    throw new Error(
      `Customer address on ${orderId} has no detectable house number: ` +
        `"${asText(orderFields["Customer Address"])}"`
    );
  }

  const sellerCountryCode = await getSellerCountryCodeFromOrderFields(orderFields);

  /*
    DPD where it reaches, UPS everywhere else, and UPS again if DPD turns
    out not to serve this destination after all.

    The routing table says which countries DPD can leave from against our
    Dutch sender; it says nothing about where it can arrive. Rather than
    refuse the label over that, the parcel goes UPS and the log says why.
    A pair that ships a few euros dearer beats a pair that does not ship.
  */
  let carrier = await pickMarketplaceCarrier(sellerCountryCode);

  let method = await findSendcloudShippingMethod({
    carrier,
    toCountry: customerAddress.country,
    senderAddressId: SENDCLOUD_MARKETPLACE_SENDER_ADDRESS_ID
  }).catch((error) => {
    if (carrier === "UPS") throw error;

    console.warn(`${orderId}: no DPD to ${customerAddress.country}, falling back to UPS`);

    carrier = "UPS";

    return null;
  });

  if (!method) {
    method = await findSendcloudShippingMethod({
      carrier,
      toCountry: customerAddress.country,
      senderAddressId: SENDCLOUD_MARKETPLACE_SENDER_ADDRESS_ID
    });
  }

  console.log(
    `${orderId}: consignor in ${sellerCountryCode || "?"} -> ${carrier} ` +
      `"${method.name}" (#${method.id}) to ${customerAddress.country}`
  );

  /*
    Everything decided, nothing spent.

    The two things that can only be answered by Sendcloud - does this method
    exist on this lane, and will they take this address - are split by the
    parcel call, and only the second one costs money. So a dry run answers
    the first, shows exactly what the second would be handed, and stops. It
    is also the only way to see whether the sender address is configured
    without opening a finished label to read the name off it.
  */
  if (dry) {
    return {
      ok: true,
      dry: true,
      order: orderId,
      consignorCountry: sellerCountryCode || null,
      carrier,
      method: `${method.name} (#${method.id})`,
      senderAddressId: SENDCLOUD_MARKETPLACE_SENDER_ADDRESS_ID || "NOT SET - would use the account default",
      weightKg: SENDCLOUD_MARKETPLACE_WEIGHT_KG,
      shipTo: {
        ...customerAddress,
        phone: customerAddress.phone || SENDCLOUD_MARKETPLACE_FALLBACK_PHONE
      },
      contractId: contractId || "account default",
      contracts: await listSendcloudContracts(carrier.toLowerCase()).catch((error) => ({ error: error.message })),
      wouldDeliverTo:
        asText(orderFields["Claimed Channel ID"]) || "the consignor's own labels channel"
    };
  }

  const sendcloud = await createSendcloudLabel({
    customerAddress,
    shippingOptionCode: method.id,
    orderId,
    storeName: asText(orderFields["Marketplace"]) || asText(orderFields["Store Name"]),
    shopifyOrderNumber: asText(orderFields["Shopify Order Number"]),
    senderAddressId: SENDCLOUD_MARKETPLACE_SENDER_ADDRESS_ID,
    weightKg: SENDCLOUD_MARKETPLACE_WEIGHT_KG,
    fallbackPhone: SENDCLOUD_MARKETPLACE_FALLBACK_PHONE,
    contractId
  });

  const labelPdfBuffer = await fetchBuffer(sendcloud.labelUrl, {
    Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
  });

  const uploadedPdfUrl = await uploadPdfToR2({
    key: `shipping-labels/${sanitizeFileName(orderId)}.pdf`,
    pdfBuffer: labelPdfBuffer
  });

  await airtable(AIRTABLE_UNFULFILLED_ORDERS_LOG_TABLE).update(orderRecord.id, {
    "Fulfillment Status": "Requested Label",
    "Tracking Number": sendcloud.trackingNumber,
    /*
      Recorded because a marketplace has to be told who is carrying it, and
      working it out again later gets it wrong in exactly the case the
      fallback above exists for: DPD chosen on the consignor's country, UPS
      actually used because DPD does not reach the shopper. Reported wrong,
      bol shows the buyer a courier that never had the parcel.
    */
    "Shipping Carrier": carrier,
    "Shipping Label": [
      { url: uploadedPdfUrl, filename: `${sanitizeFileName(orderId)}.pdf` }
    ],
    "Label Error Message": null
  });

  /*
    Delivered, not stored. A label nobody is told about is the same as no
    label at all, and this is the step that went missing when bol orders
    fell through to the store path.

    Whoever is actually shipping gets it, and that is not always the
    consignor. A deal nobody took becomes a quick deal, and then the claimer
    holds the pair and reads the deal channel - so that channel wins when it
    exists, exactly as it does for a store order.
  */
  const claimedChannelId = asText(orderFields["Claimed Channel ID"]);

  const sellerRecord = claimedChannelId
    ? null
    : await getSellerRecordFromLinkedSellerValue(
        first(orderFields["Linked Seller ID"])
      ).catch(() => null);

  const channelId =
    claimedChannelId || asText(sellerRecord?.fields?.["Labels Channel ID"]);

  const delivery = {
    orderId,
    trackingNumber: sendcloud.trackingNumber,
    labelUrl: uploadedPdfUrl,
    productName: asText(orderFields["Product Name"]),
    sku: asText(orderFields["SKU (Soft)"]) || asText(orderFields["SKU"]),
    size: asText(orderFields["Size"])
  };

  if (!claimedChannelId && isStoreSellerRecord(sellerRecord)) {
    await sendLabelToLojiqStoreChannel({ channelId, ...delivery });
  } else if (channelId) {
    await sendFinalLabelToDiscordChannel({ channelId, ...delivery });
  } else {
    await sendFinalLabelToDiscordDM({
      discordUserId: asText(sellerRecord?.fields?.["Discord ID"]),
      ...delivery
    });
  }

  return {
    ok: true,
    message: `Label created for ${orderId}`,
    carrier,
    method: method.name,
    tracking_number: sendcloud.trackingNumber
  };
}

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

    if (!["quick_deal", "wtb_deal", "marketplace"].includes(source)) {
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

    /*
      Nothing below this line applies to a marketplace sale.

      The rest of the route asks a store for a label, or makes one from the
      store's own Shopify order. A bol sale has neither: no store to ask, no
      Shopify order to read, and the shopper's address already on our record.

      Decided on the order rather than on which button was pressed. A bol
      sale can arrive here twice over: the consignor presses Request Label
      on his own deal, or nobody took it, it became a quick deal and whoever
      claimed it presses the button in that channel instead. Same parcel to
      the same shopper either way, so trusting the caller's own word for it
      would only mean the second route fails on a Shopify order that was
      never there.
    */
    const isBolOrder = asText(orderFields["Marketplace"]).toLowerCase() === "bol";

    if (source === "marketplace" || isBolOrder) {
      return res.status(200).json(
        await createMarketplaceLabel({
          orderRecord,
          orderFields,
          orderId,
          dry: req.body?.dry === true,
          contractId: /^\d+$/.test(asText(req.body?.contract_id)) ? asText(req.body.contract_id) : null
        })
      );
    }

    const clientId = first(orderFields["Client"]);
    if (!clientId) {
      throw new Error(`The Order ${orderId} has no linked Client`);
    }

    const dealChannelId =
      source === "wtb_deal"
        ? asText(orderFields["WTB Created Channel ID"])
        : asText(orderFields["Claimed Channel ID"]);
    
    if (!dealChannelId) {
      throw new Error(
        source === "wtb_deal"
          ? `Missing WTB Created Channel ID for order ${orderId}`
          : `Missing Claimed Channel ID for order ${orderId}`
      );
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
      channelId: dealChannelId,
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
    
    let targetChannelId = claimedChannelId || wtbChannelId;
    let markLabelOk = true;
    let sellerDiscordId = "";
    let storeSeller = false;
    
    if (!targetChannelId) {
      const linkedInventoryUnitIds = Array.isArray(fields["Linked Inventory Unit"])
        ? fields["Linked Inventory Unit"]
        : [];
    
      if (!linkedInventoryUnitIds.length) {
        throw new Error("No channel ID found and no linked inventory unit found");
      }
    
      const inventoryRecord = await airtable(AIRTABLE_INVENTORY_UNITS_TABLE)
        .find(linkedInventoryUnitIds[0]);
    
      const sellerRecordId = Array.isArray(inventoryRecord.fields["Seller ID"])
        ? inventoryRecord.fields["Seller ID"][0]
        : "";
    
      if (!sellerRecordId) {
        throw new Error("No seller linked to inventory unit");
      }
    
      const sellerRecord = await airtable(AIRTABLE_SELLERS_TABLE).find(sellerRecordId);
      storeSeller = isStoreSellerRecord(sellerRecord);
      targetChannelId = asText(sellerRecord.fields["Labels Channel ID"]);
      sellerDiscordId = asText(sellerRecord.fields["Discord ID"]);
      markLabelOk = false;
      
      if (!targetChannelId && !sellerDiscordId) {
        throw new Error("No channel ID found and seller has no Labels Channel ID or Discord ID");
      }
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
    if (storeSeller) {
      await sendLabelToLojiqStoreChannel({
        channelId: targetChannelId,
        orderId: asText(fields["Order ID"]) || record.id,
        trackingNumber,
        labelUrl,
        productName: asText(fields["Product Name"]),
        sku: asText(fields["SKU"]),
        size: asText(fields["Size"])
      });
    } else if (targetChannelId) {
      await sendFinalLabelToDiscordChannel({
        channelId: targetChannelId,
        orderId: asText(fields["Order ID"]) || record.id,
        trackingNumber,
        labelUrl,
        productName: asText(fields["Product Name"]),
        sku: asText(fields["SKU"]),
        size: asText(fields["Size"]),
        markLabelOk
      });
    } else {
      await sendFinalLabelToDiscordDM({
        discordUserId: sellerDiscordId,
        orderId: asText(fields["Order ID"]) || record.id,
        trackingNumber,
        labelUrl,
        productName: asText(fields["Product Name"]),
        sku: asText(fields["SKU"]),
        size: asText(fields["Size"])
      });
    }

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

app.use("/api/returns", returnsRouter);

app.listen(PORT, () => {
  console.log(`Lojiq WMS running on port ${PORT}`);
});
