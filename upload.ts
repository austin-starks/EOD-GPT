import Database from "./src/services/databases/mongo";
import PriceMetrics from "./src/models/PriceMetrics";
import StockFinancials from "./src/models/StockFinancials";

class EarningsProcessor {
  private static getTickersFromCsv(): string[] {
    const fs = require("fs");
    const csvContent = fs.readFileSync("tickers.csv", "utf-8");
    return csvContent
      .split("\n")
      .slice(1) // Skip header row
      .map((line) => line.trim()) // Get ticker from line
      .filter((ticker) => ticker && ticker.length > 0); // Remove empty lines
  }

  async processAndSaveEarningsForOneStock(ticker: string) {
    await StockFinancials.downloadFinancials(ticker);
  }

  async processAndSaveEarningsForAllStocks() {
    const tickers = EarningsProcessor.getTickersFromCsv();
    await StockFinancials.downloadFinancialsForTickerList(tickers);
  }

  async updateAllFinancials() {
    const tickers = EarningsProcessor.getTickersFromCsv();
    await StockFinancials.downloadFinancialsForTickerList(tickers);
  }
}

(async () => {
  try {
    const cloud = new Database("local");
    await cloud.connect();
    console.log("Connected to database");
    const startTime = new Date();
    const processor = new EarningsProcessor();
    await processor.processAndSaveEarningsForAllStocks();
    await PriceMetrics.hydrateAllData();
    const endTime = new Date();
    const duration = endTime.getTime() - startTime.getTime();
    console.log(
      `Processing complete. Files have been saved. Time taken: ${duration}ms`
    );
  } catch (error) {
    console.error("Error:", error);
  } finally {
    process.exit(0);
  }
})();
