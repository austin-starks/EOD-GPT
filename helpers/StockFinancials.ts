import { EodhdBulkFundamentalsResponse, EodhdClient } from "./eodhdClient";
import { Schema, model } from "mongoose";

import { FinancialsDataManager } from "./bigQuery";
import momentTz from "moment-timezone";

// Batch size for processing stocks in groups
const BATCH_SIZE = 10;

// Define the interface with required fields
export interface IStockFinancials {
  date: Date;
  ticker: string;
  symbol: string;
  [key: string]: any; // Allow any additional fields
}

class NoFinancialsError extends Error {
  constructor() {
    super("Earnings don't exist");
  }
}
class EtfError extends Error {
  constructor() {
    super("ETF data exists");
  }
}

// Schema definition with required fields
const baseSchema = {
  date: { type: Date, required: true },
  ticker: { type: String, required: true },
  symbol: { type: String, required: true },
};

const schemaOptions = {
  strict: false, // Allow additional fields not specified in schema
};

const QuarterlyFinancialsSchema = new Schema<IStockFinancials>(
  baseSchema,
  schemaOptions
);
const AnnualFinancialsSchema = new Schema<IStockFinancials>(
  baseSchema,
  schemaOptions
);

// Create compound indexes for efficient lookups
QuarterlyFinancialsSchema.index({ ticker: 1, date: 1 }, { unique: true });
AnnualFinancialsSchema.index({ ticker: 1, date: 1 }, { unique: true });

const QuarterlyFinancialsModel = model<IStockFinancials>(
  "StockQuarterlyFinancials",
  QuarterlyFinancialsSchema
);
const AnnualFinancialsModel = model<IStockFinancials>(
  "StockAnnualFinancials",
  AnnualFinancialsSchema
);

// Meta model to track last download timestamp
export interface IStockFinancialsMeta {
  _id: string;
  lastDownload: Date;
}

export default class StockFinancials {
  private static getClose(datestring: string | Date) {
    return momentTz
      .tz(datestring, "America/New_York")
      .set({ hour: 16, minute: 0, second: 0, millisecond: 0 })
      .utc()
      .toDate();
  }

  private static processFinancialData(
    ticker: string,
    dictKey: string,
    details: any
  ) {
    return {
      ...details,
      ticker,
      symbol: ticker,
      date: this.getClose(details?.filing_date || dictKey),
      filing_date: undefined,
    };
  }

  // Only allow strictly numeric fields (plus the three required).
  private static generateBigQuerySchema(data: any[]) {
    const requiredFields = [
      { name: "ticker", type: "STRING" },
      { name: "symbol", type: "STRING" },
      { name: "date", type: "TIMESTAMP" },
    ];
    const numericFields = new Set<string>();

    data.forEach((item) => {
      for (const [key, value] of Object.entries(item)) {
        if (!["ticker", "symbol", "date"].includes(key)) {
          // Only add this key if it's truly a number
          if (typeof value === "number") {
            numericFields.add(key);
          }
        }
      }
    });

    const schema = [...requiredFields];
    for (const field of numericFields) {
      schema.push({ name: field, type: "FLOAT64" });
    }

    return schema;
  }

  // Filter a record so that it only contains keys defined in the schema.
  private static filterRecordForBQ(
    record: IStockFinancials,
    schema: { name: string; type: string }[]
  ): any {
    const allowedFields = new Set(schema.map((field) => field.name));
    const filtered: any = {};
    for (const key of Object.keys(record)) {
      if (allowedFields.has(key)) {
        filtered[key] = record[key];
      }
    }
    return filtered;
  }

  private static async fetchFinancials(
    ticker: string
  ): Promise<{ quarterly: IStockFinancials[]; annual: IStockFinancials[] }> {
    console.log(`Downloading financials for ${ticker}...`);
    const client = new EodhdClient();

    const fundamentals = await client.getFundamentals(ticker);
    if (!fundamentals) {
      throw new NoFinancialsError();
    }
    if (fundamentals.ETF_Data || fundamentals.General?.Type === "ETF") {
      throw new EtfError();
    }
    if (
      !fundamentals.General?.Address ||
      !fundamentals.General.Address.includes("United States")
    ) {
      throw new Error("Non-US or missing address");
    }
    if (Object.keys(fundamentals.Financials || {}).length === 0) {
      throw new NoFinancialsError();
    }

    // Check if any of the financial statements use non-USD currency
    const currencySymbols = [
      fundamentals.Financials?.Balance_Sheet?.currency_symbol,
      fundamentals.Financials?.Cash_Flow?.currency_symbol,
      fundamentals.Financials?.Income_Statement?.currency_symbol,
    ];

    if (currencySymbols.some((symbol) => symbol && symbol !== "USD")) {
      throw new Error("Non-USD currency detected");
    }

    const quarterlyMap = new Map<string, any>();
    const annualMap = new Map<string, any>();

    const quarterlyData = {
      Balance_Sheet: fundamentals?.Financials?.Balance_Sheet?.quarterly || {},
      Cash_Flow: fundamentals?.Financials?.Cash_Flow?.quarterly || {},
      Income_Statement:
        fundamentals?.Financials?.Income_Statement?.quarterly || {},
    };

    Object.values(quarterlyData).forEach((statementObj) => {
      if (!statementObj) return;
      Object.entries(statementObj).forEach(([dictKey, details]) => {
        const record = this.processFinancialData(ticker, dictKey, details);
        const key = record.date.toISOString();
        this.mergeRecord(quarterlyMap, key, record);
      });
    });

    const yearlyData = {
      Balance_Sheet: fundamentals?.Financials?.Balance_Sheet?.yearly || {},
      Cash_Flow: fundamentals?.Financials?.Cash_Flow?.yearly || {},
      Income_Statement:
        fundamentals?.Financials?.Income_Statement?.yearly || {},
    };

    Object.values(yearlyData).forEach((statementObj) => {
      if (!statementObj) return;
      Object.entries(statementObj).forEach(([dictKey, details]) => {
        const record = this.processFinancialData(ticker, dictKey, details);
        const key = record.date.toISOString();
        this.mergeRecord(annualMap, key, record);
      });
    });

    return {
      quarterly: Array.from(quarterlyMap.values()),
      annual: Array.from(annualMap.values()),
    };
  }

  private static async updateDatabaseAndBQ(
    quarterly: IStockFinancials[],
    annual: IStockFinancials[]
  ) {
    // 1. Update MongoDB (store full records).
    const bulkOpsQuarterly = quarterly.map((doc) => ({
      updateOne: {
        filter: { ticker: doc.ticker, date: doc.date },
        update: doc,
        upsert: true,
      },
    }));
    const bulkOpsAnnual = annual.map((doc) => ({
      updateOne: {
        filter: { ticker: doc.ticker, date: doc.date },
        update: doc,
        upsert: true,
      },
    }));

    if (bulkOpsQuarterly.length > 0) {
      await QuarterlyFinancialsModel.bulkWrite(bulkOpsQuarterly);
    }
    if (bulkOpsAnnual.length > 0) {
      await AnnualFinancialsModel.bulkWrite(bulkOpsAnnual);
    }

    // 2. Update BigQuery with only numeric fields + required fields
    try {
      if (quarterly.length > 0) {
        const quarterlySchema = this.generateBigQuerySchema(quarterly);
        const quarterlyBQ = await FinancialsDataManager.createQuarterlyTable(
          quarterlySchema
        );
        const filteredQuarterly = quarterly.map((record) =>
          this.filterRecordForBQ(record, quarterlySchema)
        );
        await quarterlyBQ.insertFinancialsToTempTable(filteredQuarterly);
        await quarterlyBQ.mergeTempTableToMainTable();
        console.log("Quarterly financials uploaded to BigQuery successfully");
      }

      if (annual.length > 0) {
        const annualSchema = this.generateBigQuerySchema(annual);
        const annualBQ = await FinancialsDataManager.createAnnualTable(
          annualSchema
        );
        const filteredAnnual = annual.map((record) =>
          this.filterRecordForBQ(record, annualSchema)
        );
        await annualBQ.insertFinancialsToTempTable(filteredAnnual);
        await annualBQ.mergeTempTableToMainTable();
        console.log("Annual financials uploaded to BigQuery successfully");
      }
    } catch (error) {
      console.error("Error uploading to BigQuery:", error);
      console.log(error?.errors?.[0]);
    }
  }

  // downloadFinancials for single ticker
  public static async downloadFinancials(ticker: string): Promise<{
    quarterly: IStockFinancials[];
    annual: IStockFinancials[];
  }> {
    const { quarterly, annual } = await this.fetchFinancials(ticker);
    await this.updateDatabaseAndBQ(quarterly, annual);
    return { quarterly, annual };
  }

  private static mergeRecord(
    map: Map<string, any>,
    key: string | undefined,
    newData: any
  ) {
    if (!key) return;
    const existing = map.get(key) || {};
    map.set(key, { ...existing, ...newData });
  }

  // New method to process tickers in bulk
  public static async downloadFinancialsForTickerList(tickers: string[]) {
    const client = new EodhdClient();
    const batchSize = 500; // Bulk API limit
    console.log("Total stocks to process:", tickers.length);
    const totalBatches = Math.ceil(tickers.length / batchSize);

    for (let i = 0; i < tickers.length; i += batchSize) {
      const currentBatch = Math.floor(i / batchSize) + 1;
      console.log(`Processing Batch ${currentBatch}/${totalBatches}`);
      const batch = tickers.slice(i, i + batchSize);
      const batchQuarterly: IStockFinancials[] = [];
      const batchAnnual: IStockFinancials[] = [];

      try {
        const bulkData: EodhdBulkFundamentalsResponse[] =
          await client.getBulkFundamentals("US", batch, 0, batchSize);

        for (const fundamentals of bulkData) {
          const ticker = fundamentals.General?.Code;
          if (!ticker) continue;
          if (fundamentals.ETF_Data || fundamentals.General?.Type === "ETF")
            continue;
          if (
            !fundamentals.General?.Address ||
            !fundamentals.General.Address.includes("United States")
          )
            continue;
          if (
            !fundamentals.Financials ||
            Object.keys(fundamentals.Financials).length === 0
          )
            continue;

          try {
            const { quarterly, annual } = this.processBulkFinancialData(
              ticker,
              fundamentals
            );
            batchQuarterly.push(...quarterly);
            batchAnnual.push(...annual);
            console.log(`Processed ${ticker} successfully`);
          } catch (error) {
            console.error(`Error processing ${ticker}:`, error);
          }
        }

        if (batchQuarterly.length > 0 || batchAnnual.length > 0) {
          await this.updateDatabaseAndBQ(batchQuarterly, batchAnnual);
          console.log(`Batch ${currentBatch} uploaded successfully`);
        }

        // Add a small delay between batches to avoid rate limiting
        if (currentBatch < totalBatches) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      } catch (error) {
        console.error(
          `Error fetching bulk fundamentals for batch ${batch.join(", ")}:`,
          error
        );
      }
    }
  }

  // Add this new helper method to process bulk fundamentals data
  private static processBulkFinancialData(
    ticker: string,
    fundamentals: EodhdBulkFundamentalsResponse
  ): { quarterly: IStockFinancials[]; annual: IStockFinancials[] } {
    if (fundamentals.ETF_Data || fundamentals.General?.Type === "ETF") {
      throw new EtfError();
    }
    if (
      !fundamentals.General?.Address ||
      !fundamentals.General.Address.includes("United States")
    ) {
      throw new Error("Non-US or missing address");
    }
    if (
      !fundamentals.Financials ||
      Object.keys(fundamentals.Financials).length === 0
    ) {
      throw new NoFinancialsError();
    }

    // Check currency symbols for all financial statements
    const currencySymbols = [
      fundamentals.Financials.Balance_Sheet?.currency_symbol,
      fundamentals.Financials.Cash_Flow?.currency_symbol,
      fundamentals.Financials.Income_Statement?.currency_symbol,
    ];

    if (currencySymbols.some((symbol) => symbol && symbol !== "USD")) {
      throw new Error("Non-USD currency detected");
    }

    const quarterlyMap = new Map<string, any>();
    const annualMap = new Map<string, any>();
    const financialStatements = [
      "Balance_Sheet",
      "Cash_Flow",
      "Income_Statement",
    ];

    financialStatements.forEach((statement) => {
      const stmtData = fundamentals.Financials?.[statement];
      if (stmtData) {
        // Process quarterly data
        [
          "quarterly_last_0",
          "quarterly_last_1",
          "quarterly_last_2",
          "quarterly_last_3",
        ].forEach((key) => {
          if (stmtData[key]) {
            const record = this.processFinancialData(
              ticker,
              key,
              stmtData[key]
            );
            this.mergeRecord(quarterlyMap, record.date.toISOString(), record);
          }
        });
        // Process yearly data
        [
          "yearly_last_0",
          "yearly_last_1",
          "yearly_last_2",
          "yearly_last_3",
        ].forEach((key) => {
          if (stmtData[key]) {
            const record = this.processFinancialData(
              ticker,
              key,
              stmtData[key]
            );
            this.mergeRecord(annualMap, record.date.toISOString(), record);
          }
        });
      }
    });

    return {
      quarterly: Array.from(quarterlyMap.values()),
      annual: Array.from(annualMap.values()),
    };
  }
}
