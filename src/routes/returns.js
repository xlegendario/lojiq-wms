import express from "express";
import Airtable from "airtable";
import QRCode from "qrcode";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const router = express.Router();

const {
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_ORDERS_TABLE = "Unfulfilled Orders Log",
  AIRTABLE_RETURNS_TABLE = "Incoming Returns",
  AIRTABLE_RETURNS_TABLE_ID,
  AIRTABLE_MERCHANTS_TABLE = "Merchants",
  AIRTABLE_RETURN_METHODS_TABLE = "Return Shipping Methods",
  SENDCLOUD_PUBLIC_KEY,
  SENDCLOUD_SECRET_KEY,
  SENDCLOUD_RETURNS_URL = "https://panel.sendcloud.sc/api/v3/returns",
  SENDCLOUD_PARCEL_TRACKING_BASE_URL = "https://panel.sendcloud.sc/api/v3/parcels/tracking",
  SENDCLOUD_TO_NAME,
  SENDCLOUD_TO_COMPANY,
  SENDCLOUD_TO_ADDRESS_1,
  SENDCLOUD_TO_CITY,
  SENDCLOUD_TO_POSTAL_CODE,
  SENDCLOUD_TO_COUNTRY = "NL",
  SENDCLOUD_TO_EMAIL,
  SENDCLOUD_TO_PHONE,
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE_URL,
  APP_PUBLIC_BASE_URL,
  MAKE_MANUAL_RETURN_WEBHOOK_URL,
  DISCORD_BOT_BASE_URL
} = process.env;

const required = [
  "AIRTABLE_TOKEN",
  "AIRTABLE_BASE_ID",
  "AIRTABLE_RETURNS_TABLE_ID",
  "SENDCLOUD_PUBLIC_KEY",
  "SENDCLOUD_SECRET_KEY",
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
  "R2_PUBLIC_BASE_URL",
  "APP_PUBLIC_BASE_URL"
];

for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required env var: ${key}`);
  }
}

const airtable = new Airtable({ apiKey: AIRTABLE_TOKEN }).base(AIRTABLE_BASE_ID);

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY
  }
});
