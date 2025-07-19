import { Document, Schema, model } from "mongoose";

import { EodhdClient } from "../services/fundamentalApi/EodhdClient";
import InternationalStock from "./InternationalStock";
import { PriceMetricsDataManager } from "../services/bigQueryClient";
import { QuarterlyFinancialsModel } from "./StockFinancials";
import moment from "moment";
import momentTz from "moment-timezone";

// Interface for PriceMetrics
export interface IPriceMetrics extends Document {
  ticker: string;
  symbol: string;
  date: Date;
  price: number;
  volume: number;
  marketCap: number;

  // US-only metrics
  peRatioTTM?: number | null;
  psRatioTTM?: number | null;
  pbRatioTTM?: number | null;
  enterpriseValue?: number | null;

  // Metadata
  isInternational: boolean;
  lastUpdated: Date;
}

// Schema definition
const PriceMetricsSchema = new Schema<IPriceMetrics>(
  {
    ticker: { type: String, required: true },
    symbol: { type: String, required: true },
    date: { type: Date, required: true },
    price: { type: Number, required: true },
    volume: { type: Number, required: true },
    marketCap: { type: Number, required: true },

    // US-only metrics
    peRatioTTM: { type: Number, default: null },
    psRatioTTM: { type: Number, default: null },
    pbRatioTTM: { type: Number, default: null },
    enterpriseValue: { type: Number, default: null },

    // Metadata
    isInternational: { type: Boolean, required: true },
    lastUpdated: { type: Date, default: Date.now },
  },
  {
    timestamps: true,
  }
);

// Indexes
PriceMetricsSchema.index({ ticker: 1, date: -1 }, { unique: true });
PriceMetricsSchema.index({ isInternational: 1 });
PriceMetricsSchema.index({ lastUpdated: -1 });

// Model
export const PriceMetricsModel = model<IPriceMetrics>(
  "StockPriceMetrics",
  PriceMetricsSchema
);

export default class PriceMetrics {
  // 1. For international stocks, we just want to compute market cap, volume, and price
  // 2. For US stocks, we want to compute P/E ratio TTM, market cap, P/S ratio TTM, P/B ratio TTM,Enterprise Value (EV) = Market Cap + Total Debt - Cash and Cash Equivalents

  /**
   * Standardize date to 4pm EST (market close)
   */
  private static standardizeToMarketClose(date: Date): Date {
    // Convert to moment timezone object in EST
    const estDate = momentTz(date).tz("America/New_York");

    // Set time to 4pm EST
    estDate.hours(16).minutes(0).seconds(0).milliseconds(0);

    // Convert to UTC and return as Date
    return estDate.utc().toDate();
  }

  /**
   * Get BigQuery schema for PriceMetrics
   */
  static getBigQuerySchema() {
    return [
      { name: "ticker", type: "STRING", mode: "REQUIRED" },
      { name: "symbol", type: "STRING", mode: "REQUIRED" },
      { name: "date", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "price", type: "FLOAT64", mode: "REQUIRED" },
      { name: "volume", type: "FLOAT64", mode: "REQUIRED" },
      { name: "marketCap", type: "FLOAT64", mode: "REQUIRED" },
      { name: "peRatioTTM", type: "FLOAT64", mode: "NULLABLE" },
      { name: "psRatioTTM", type: "FLOAT64", mode: "NULLABLE" },
      { name: "pbRatioTTM", type: "FLOAT64", mode: "NULLABLE" },
      { name: "enterpriseValue", type: "FLOAT64", mode: "NULLABLE" },
      { name: "isInternational", type: "BOOLEAN", mode: "REQUIRED" },
      { name: "lastUpdated", type: "TIMESTAMP", mode: "REQUIRED" },
    ];
  }

  /**
   * Get historical price data for a ticker using EODHD
   */
  private static async getHistoricalPriceData(
    ticker: string,
    from: string,
    to: string,
    exchange: string = "US"
  ): Promise<
    Array<{
      price: number;
      volume: number;
      date: Date;
    }>
  > {
    try {
      const eodhdClient = new EodhdClient();
      const historicalData = await eodhdClient.getHistoricalPriceData(
        ticker,
        exchange,
        from,
        to,
        "d"
      );

      return historicalData.map((data) => ({
        price: data.close, // Close price
        volume: data.volume, // Volume
        date: this.standardizeToMarketClose(new Date(data.date)), // Convert to 4pm EST
      }));
    } catch (error) {
      console.error(
        `Error fetching historical price data for ${ticker}.${exchange}:`,
        error
      );
      return [];
    }
  }

  /**
   * Calculate metrics for historical data points
   */
  private static async calculateHistoricalMetrics(
    ticker: string,
    isInternational: boolean,
    from: string,
    to: string,
    exchange: string = "US"
  ): Promise<IPriceMetrics[]> {
    try {
      // Get historical price data
      const priceDataPoints = await this.getHistoricalPriceData(
        ticker,
        from,
        to,
        exchange
      );
      if (priceDataPoints.length === 0) {
        console.log(`No historical price data available for ${ticker}`);
        return [];
      }

      const metrics: IPriceMetrics[] = [];

      if (isInternational) {
        // Get all historical shares outstanding data for international stock
        const intlStockData = await InternationalStock.find({ ticker })
          .sort({ date: 1 })
          .lean();

        if (!intlStockData || intlStockData.length === 0) {
          console.log(
            `No shares outstanding data for international stock ${ticker}`
          );
          return [];
        }

        // For each price data point, find the most recent shares outstanding data
        for (const priceData of priceDataPoints) {
          // Find the most recent shares outstanding data as of this price date
          let closestSharesData = null;
          for (const sharesData of intlStockData) {
            if (sharesData.date <= priceData.date) {
              closestSharesData = sharesData;
            } else {
              break;
            }
          }

          if (
            closestSharesData &&
            closestSharesData.commonStockSharesOutstanding
          ) {
            metrics.push({
              ticker,
              symbol: ticker,
              date: priceData.date, // Already standardized from getHistoricalPriceData
              price: priceData.price,
              volume: priceData.volume,
              marketCap:
                priceData.price *
                closestSharesData.commonStockSharesOutstanding,
              peRatioTTM: null,
              psRatioTTM: null,
              pbRatioTTM: null,
              enterpriseValue: null,
              isInternational: true,
              lastUpdated: new Date(),
            } as IPriceMetrics);
          }
        }
      } else {
        // US Stock - get all quarterly financials
        const allQuarters = await QuarterlyFinancialsModel.find({ ticker })
          .sort({ date: 1 })
          .lean();

        if (!allQuarters || allQuarters.length === 0) {
          console.log(`No financial data available for US stock ${ticker}`);
          return [];
        }

        // For each price data point, calculate metrics based on available financial data
        for (const priceData of priceDataPoints) {
          // Use all quarters that were reported before or on this price date
          const availableQuarters = allQuarters.filter(
            (q: any) => q.date <= priceData.date
          );

          if (availableQuarters.length === 0) continue;

          // Get the latest quarter as of this price date
          const latestQuarter = availableQuarters[availableQuarters.length - 1];

          if (!latestQuarter.commonStockSharesOutstanding) continue;

          const sharesOutstanding = latestQuarter.commonStockSharesOutstanding;
          const marketCap = priceData.price * sharesOutstanding;

          // Get TTM data (last 4 quarters or scale if less)
          const ttmQuarters = availableQuarters.slice(-4);
          const quartersAvailable = ttmQuarters.length;

          // Calculate TTM values with scaling
          const sumNetIncome = ttmQuarters.reduce(
            (sum: number, q: any) => sum + (q.netIncome || 0),
            0
          );
          const sumTotalRevenue = ttmQuarters.reduce(
            (sum: number, q: any) => sum + (q.totalRevenue || 0),
            0
          );
          const scaleFactor = quartersAvailable > 0 ? 4 / quartersAvailable : 0;

          const ttmNetIncome = sumNetIncome * scaleFactor;
          const ttmTotalRevenue = sumTotalRevenue * scaleFactor;

          // Calculate ratios
          const peRatioTTM = ttmNetIncome > 0 ? marketCap / ttmNetIncome : null;
          const psRatioTTM =
            ttmTotalRevenue > 0 ? marketCap / ttmTotalRevenue : null;
          const pbRatioTTM =
            latestQuarter.totalStockholderEquity > 0
              ? marketCap / latestQuarter.totalStockholderEquity
              : null;

          // Calculate Enterprise Value only if debt fields exist
          let enterpriseValue: number | null = null;
          if (
            latestQuarter.longTermDebt !== undefined ||
            latestQuarter.shortTermDebt !== undefined ||
            latestQuarter.shortLongTermDebt !== undefined ||
            latestQuarter.cashAndShortTermInvestments !== undefined ||
            latestQuarter.cash !== undefined
          ) {
            const totalDebt =
              (latestQuarter.longTermDebt || 0) +
              (latestQuarter.shortTermDebt || 0) +
              (latestQuarter.shortLongTermDebt || 0);
            const cash =
              latestQuarter.cashAndShortTermInvestments ||
              latestQuarter.cash ||
              0;
            enterpriseValue = marketCap + totalDebt - cash;
          }

          metrics.push({
            ticker,
            symbol: ticker,
            date: priceData.date, // Already standardized from getHistoricalPriceData
            price: priceData.price,
            volume: priceData.volume,
            marketCap,
            peRatioTTM,
            psRatioTTM,
            pbRatioTTM,
            enterpriseValue,
            isInternational: false,
            lastUpdated: new Date(),
          } as IPriceMetrics);
        }
      }

      return metrics;
    } catch (error) {
      console.error(
        `Error calculating historical metrics for ${ticker}:`,
        error
      );
      return [];
    }
  }

  /**
   * Save metrics to MongoDB and BigQuery
   */
  private static async saveMetrics(metrics: IPriceMetrics[]): Promise<void> {
    if (metrics.length === 0) return;

    try {
      // Save to MongoDB
      const bulkOps = metrics.map((metric) => ({
        updateOne: {
          filter: { ticker: metric.ticker, date: metric.date },
          update: { $set: metric },
          upsert: true,
        },
      }));
      await PriceMetricsModel.bulkWrite(bulkOps);
      console.log(`Saved ${metrics.length} price metrics to MongoDB`);

      // Save to BigQuery
      const bqManager = await PriceMetricsDataManager.createPriceMetricsTable();
      await bqManager.insertPriceMetricsToTempTable(metrics);
      await bqManager.mergeTempTableToMainTable();
      console.log(`Saved ${metrics.length} price metrics to BigQuery`);
    } catch (error) {
      console.error("Error saving price metrics:", error);
      throw error;
    }
  }

  static async hydrateAllData() {
    try {
      console.log("Starting full price data hydration...");

      // Get all US tickers
      const usTickers = await QuarterlyFinancialsModel.distinct("ticker");
      console.log(`Found ${usTickers.length} US stocks`);

      // Get all international tickers
      const intlTickers = await InternationalStock.distinct("ticker");
      console.log(`Found ${intlTickers.length} international stocks`);

      // Create a set of international tickers for quick lookup
      const intlTickerSet = new Set(intlTickers);

      // Process all tickers in batches
      const allTickers = Array.from(new Set([...usTickers, ...intlTickers]));
      const batchSize = 10; // Smaller batch size for historical data

      // Define date range for historical data
      const endDate = moment().format("YYYY-MM-DD");
      const startDate = moment().subtract(30, "years").format("YYYY-MM-DD"); // 30 years of historical data

      for (let i = 0; i < allTickers.length; i += batchSize) {
        const batch = allTickers.slice(i, i + batchSize);
        const allMetrics: IPriceMetrics[] = [];

        // Process batch in parallel
        const results = await Promise.all(
          batch.map(async (ticker) => {
            const isInternational = intlTickerSet.has(ticker);
            // For now, we default to US exchange
            // TODO: Implement logic to determine correct exchange for international stocks
            const exchange = "US";
            return await this.calculateHistoricalMetrics(
              ticker,
              isInternational,
              startDate,
              endDate,
              exchange
            );
          })
        );

        // Flatten and collect all metrics
        for (const tickerMetrics of results) {
          allMetrics.push(...tickerMetrics);
        }

        // Save batch
        if (allMetrics.length > 0) {
          await this.saveMetrics(allMetrics);
        }

        console.log(
          `Processed batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
            allTickers.length / batchSize
          )} - ${allMetrics.length} data points`
        );
      }

      console.log("Completed full price data hydration");
    } catch (error) {
      console.error("Error in hydrateAllData:", error);
      throw error;
    }
  }

  static async refreshPriceData() {
    try {
      console.log("Starting price data refresh for recent updates...");

      // Get tickers that have been updated in the last week
      const oneWeekAgo = moment().subtract(7, "days").toDate();

      // Get recently updated US tickers
      const recentUSTickers = await QuarterlyFinancialsModel.distinct(
        "ticker",
        {
          updatedAt: { $gte: oneWeekAgo },
        }
      );

      // Get recently updated international tickers
      const recentIntlTickers = await InternationalStock.distinct("ticker", {
        updatedAt: { $gte: oneWeekAgo },
      });

      // Create a set of international tickers for quick lookup
      const intlTickerSet = new Set(recentIntlTickers);

      // Combine and deduplicate
      const tickersToUpdate = Array.from(
        new Set([...recentUSTickers, ...recentIntlTickers])
      );
      console.log(`Found ${tickersToUpdate.length} tickers to update`);

      if (tickersToUpdate.length === 0) {
        console.log("No tickers to update");
        return;
      }

      // Process in batches
      const batchSize = 10;

      // Define date range for refresh - last 30 days
      const endDate = moment().format("YYYY-MM-DD");
      const startDate = moment().subtract(30, "days").format("YYYY-MM-DD");

      for (let i = 0; i < tickersToUpdate.length; i += batchSize) {
        const batch = tickersToUpdate.slice(i, i + batchSize);
        const allMetrics: IPriceMetrics[] = [];

        // Process batch in parallel
        const results = await Promise.all(
          batch.map(async (ticker) => {
            const isInternational = intlTickerSet.has(ticker);
            // For now, we default to US exchange
            // TODO: Implement logic to determine correct exchange for international stocks
            const exchange = "US";
            return await this.calculateHistoricalMetrics(
              ticker,
              isInternational,
              startDate,
              endDate,
              exchange
            );
          })
        );

        // Flatten and collect all metrics
        for (const tickerMetrics of results) {
          allMetrics.push(...tickerMetrics);
        }

        // Save batch
        if (allMetrics.length > 0) {
          await this.saveMetrics(allMetrics);
        }

        console.log(
          `Updated batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
            tickersToUpdate.length / batchSize
          )}`
        );
      }

      console.log("Completed price data refresh");
    } catch (error) {
      console.error("Error in refreshPriceData:", error);

      throw error;
    }
  }
}
