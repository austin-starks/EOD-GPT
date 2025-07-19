import { BigQuery } from "@google-cloud/bigquery";
import moment from "moment";

export class BigQueryDataManager {
  protected bigquery: BigQuery;
  
  constructor() {
    const credentials = this.setupCredentials();
    this.bigquery = new BigQuery({ credentials });
  }

  protected setupCredentials() {
    const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!credentialsJson) {
      throw new Error(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set"
      );
    }
    try {
      return JSON.parse(credentialsJson);
    } catch (error) {
      throw new Error("Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    }
  }

  static createInstance() {
    return new BigQueryDataManager();
  }

  async executeQuery(query: string): Promise<any[]> {
    try {
      const options = {
        query: query,
        location: "US",
      };
      const [job] = await this.bigquery.createQueryJob(options);
      console.log(`Job ${job.id} started.`);
      const [rows] = await job.getQueryResults();

      return rows;
    } catch (error) {
      console.error("Error executing BigQuery:", query);
      console.error(error);
      throw error;
    }
  }
}

export class EarningsDataManager {
  private bigquery: BigQuery;
  private dataset: string = "financials";
  private table: string = "quarterly_earnings";
  private tempTable: string;
  static readonly quarterlyTableId = "quarterly_earnings";

  constructor() {
    const credentials = this.setupCredentials();
    this.bigquery = new BigQuery({ credentials });
    this.tempTable = `${this.table}_temp_${Date.now()}`;
  }

  private setupCredentials() {
    const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!credentialsJson) {
      throw new Error(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set"
      );
    }
    try {
      return JSON.parse(credentialsJson);
    } catch (error) {
      throw new Error("Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    }
  }

  static async createEarningsTable(schema: any[]) {
    const instance = new EarningsDataManager();
    await instance.createTableIfNotExists(schema);
    await instance.createTempTable(schema);
    return instance;
  }

  private async createTable(tableName: string, schema?: any[]) {
    try {
      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(tableName);

      let [exists] = await table.exists();
      let iterations = 0;

      while (!exists && iterations < 3) {
        if (iterations === 0 && schema) {
          await table.create({ schema });
          console.log(`Created table ${tableName} with schema`);
        }
        iterations++;
        [exists] = await table.exists();
        await new Promise((resolve) =>
          setTimeout(resolve, 2 ** iterations * 1000)
        );
      }
    } catch (error) {
      console.error(`Error creating table ${tableName}:`, error);
      throw error;
    }
  }

  private async createTableIfNotExists(schema: any[]) {
    await this.createTable(this.table, schema);
  }

  private async createTempTable(schema: any[]) {
    await this.createTable(this.tempTable, schema);
  }

  async insertEarningsToTempTable(earnings: any[]): Promise<void> {
    try {
      if (!earnings.length) {
        console.log("No earnings data to insert");
        return;
      }

      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(this.tempTable);

      const rows = earnings.map((item) => ({
        ...item,
        date: BigQuery.timestamp(
          moment(item.date).format("YYYY-MM-DD HH:mm:ss.SSSZ")
        ),
      }));

      const batchSize = 500;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        await table.insert(batch);
        console.log(`Inserted batch of ${batch.length} earnings rows.`);
      }
    } catch (error) {
      console.error("Error inserting earnings to temp table:", error);
      throw error;
    }
  }

  async mergeTempTableToMainTable(): Promise<void> {
    try {
      const mergeQuery = `
        MERGE \`${this.dataset}.${this.table}\` T
        USING \`${this.dataset}.${this.tempTable}\` S
        ON T.ticker = S.ticker AND T.date = S.date
        WHEN MATCHED THEN
          UPDATE SET
            T.actual = S.actual,
            T.epsActual = S.epsActual,
            T.epsEstimate = S.epsEstimate,
            T.epsDifference = S.epsDifference,
            T.surprisePercent = S.surprisePercent
        WHEN NOT MATCHED THEN
          INSERT (ticker, symbol, date, actual, epsActual, epsEstimate, epsDifference, surprisePercent)
          VALUES (S.ticker, S.symbol, S.date, S.actual, S.epsActual, S.epsEstimate, S.epsDifference, S.surprisePercent)
      `;

      await this.bigquery.query({ query: mergeQuery });
      console.log("Merged temp earnings table into main table successfully.");
    } catch (error) {
      console.error("Error merging temp earnings into main table:", error);
      throw error;
    } finally {
      try {
        const dropTempTableQuery = `
          DROP TABLE IF EXISTS \`${this.dataset}.${this.tempTable}\`
        `;
        await this.bigquery.query({ query: dropTempTableQuery });
        console.log("Dropped temporary earnings table successfully.");
      } catch (dropError) {
        console.error("Error dropping temp table:", dropError);
      }
    }
  }
}

export class InternationalStockDataManager {
  private bigquery: BigQuery;
  private dataset: string = "internationalstocks";
  private table: string = "stock_data";
  private tempTable: string;

  constructor() {
    const credentials = this.setupCredentials();
    this.bigquery = new BigQuery({ credentials });
    this.tempTable = `${this.table}_temp_${Date.now()}`;
  }

  private setupCredentials() {
    const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!credentialsJson) {
      throw new Error(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set"
      );
    }
    try {
      return JSON.parse(credentialsJson);
    } catch (error) {
      throw new Error("Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    }
  }

  static async createInternationalStockTable() {
    const instance = new InternationalStockDataManager();
    const schema = [
      { name: "ticker", type: "STRING", mode: "REQUIRED" },
      { name: "symbol", type: "STRING", mode: "REQUIRED" },
      { name: "country", type: "STRING", mode: "REQUIRED" },
      { name: "commonStockSharesOutstanding", type: "FLOAT64", mode: "REQUIRED" },
      { name: "date", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
    ];
    await instance.createTableIfNotExists(schema);
    await instance.createTempTable(schema);
    return instance;
  }

  private async createTable(tableName: string, schema?: any[]) {
    try {
      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(tableName);

      let [exists] = await table.exists();
      let iterations = 0;

      while (!exists && iterations < 3) {
        if (iterations === 0 && schema) {
          await table.create({ schema });
          console.log(`Created table ${tableName} with schema`);
        }
        iterations++;
        [exists] = await table.exists();
        await new Promise((resolve) =>
          setTimeout(resolve, 2 ** iterations * 1000)
        );
      }
    } catch (error) {
      console.error(`Error creating table ${tableName}:`, error);
      throw error;
    }
  }

  private async createTableIfNotExists(schema: any[]) {
    await this.createTable(this.table, schema);
  }

  private async createTempTable(schema: any[]) {
    await this.createTable(this.tempTable, schema);
  }

  async insertInternationalStocksToTempTable(stocks: any[]): Promise<void> {
    try {
      if (!stocks.length) {
        console.log("No international stock data to insert");
        return;
      }

      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(this.tempTable);

      const rows = stocks.map((item) => ({
        ...item,
        date: BigQuery.timestamp(
          moment(item.date).format("YYYY-MM-DD HH:mm:ss.SSSZ")
        ),
      }));

      const batchSize = 500;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        await table.insert(batch);
        console.log(`Inserted batch of ${batch.length} international stock rows.`);
      }
    } catch (error) {
      console.error("Error inserting international stocks to temp table:", error);
      throw error;
    }
  }

  async mergeTempTableToMainTable(): Promise<void> {
    try {
      const mergeQuery = `
        MERGE \`${this.dataset}.${this.table}\` T
        USING \`${this.dataset}.${this.tempTable}\` S
        ON T.ticker = S.ticker AND T.date = S.date
        WHEN MATCHED THEN
          UPDATE SET
            T.country = S.country,
            T.commonStockSharesOutstanding = S.commonStockSharesOutstanding,
            T.name = S.name
        WHEN NOT MATCHED THEN
          INSERT (ticker, symbol, country, commonStockSharesOutstanding, date, name)
          VALUES (S.ticker, S.symbol, S.country, S.commonStockSharesOutstanding, S.date, S.name)
      `;

      await this.bigquery.query({ query: mergeQuery });
      console.log("Merged temp international stocks table into main table successfully.");
    } catch (error) {
      console.error("Error merging temp international stocks into main table:", error);
      throw error;
    } finally {
      try {
        const dropTempTableQuery = `
          DROP TABLE IF EXISTS \`${this.dataset}.${this.tempTable}\`
        `;
        await this.bigquery.query({ query: dropTempTableQuery });
        console.log("Dropped temporary international stocks table successfully.");
      } catch (dropError) {
        console.error("Error dropping temp table:", dropError);
      }
    }
  }
}

export class PriceMetricsDataManager {
  private bigquery: BigQuery;
  private dataset: string = "pricemetrics";
  private table: string = "stock_metrics";
  private tempTable: string;

  constructor() {
    const credentials = this.setupCredentials();
    this.bigquery = new BigQuery({ credentials });
    this.tempTable = `${this.table}_temp_${Date.now()}`;
  }

  private setupCredentials() {
    const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!credentialsJson) {
      throw new Error(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set"
      );
    }
    try {
      return JSON.parse(credentialsJson);
    } catch (error) {
      throw new Error("Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    }
  }

  static async createPriceMetricsTable() {
    const instance = new PriceMetricsDataManager();
    const schema = [
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
    await instance.createTableIfNotExists(schema);
    await instance.createTempTable(schema);
    return instance;
  }

  private async createTable(tableName: string, schema?: any[]) {
    try {
      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(tableName);

      let [exists] = await table.exists();
      let iterations = 0;

      while (!exists && iterations < 3) {
        if (iterations === 0 && schema) {
          await table.create({ schema });
          console.log(`Created table ${tableName} with schema`);
        }
        iterations++;
        [exists] = await table.exists();
        await new Promise((resolve) =>
          setTimeout(resolve, 2 ** iterations * 1000)
        );
      }
    } catch (error) {
      console.error(`Error creating table ${tableName}:`, error);
      throw error;
    }
  }

  private async createTableIfNotExists(schema: any[]) {
    await this.createTable(this.table, schema);
  }

  private async createTempTable(schema: any[]) {
    await this.createTable(this.tempTable, schema);
  }

  async insertPriceMetricsToTempTable(metrics: any[]): Promise<void> {
    try {
      if (!metrics.length) {
        console.log("No price metrics data to insert");
        return;
      }

      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(this.tempTable);

      const rows = metrics.map((item) => ({
        ...item,
        date: BigQuery.timestamp(
          moment(item.date).format("YYYY-MM-DD HH:mm:ss.SSSZ")
        ),
        lastUpdated: BigQuery.timestamp(
          moment(item.lastUpdated).format("YYYY-MM-DD HH:mm:ss.SSSZ")
        ),
      }));

      const batchSize = 500;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        await table.insert(batch);
        console.log(`Inserted batch of ${batch.length} price metrics rows.`);
      }
    } catch (error) {
      console.error("Error inserting price metrics to temp table:", error);
      throw error;
    }
  }

  async mergeTempTableToMainTable(): Promise<void> {
    try {
      const mergeQuery = `
        MERGE \`${this.dataset}.${this.table}\` T
        USING \`${this.dataset}.${this.tempTable}\` S
        ON T.ticker = S.ticker AND T.date = S.date
        WHEN MATCHED THEN
          UPDATE SET
            T.price = S.price,
            T.volume = S.volume,
            T.marketCap = S.marketCap,
            T.peRatioTTM = S.peRatioTTM,
            T.psRatioTTM = S.psRatioTTM,
            T.pbRatioTTM = S.pbRatioTTM,
            T.enterpriseValue = S.enterpriseValue,
            T.isInternational = S.isInternational,
            T.lastUpdated = S.lastUpdated
        WHEN NOT MATCHED THEN
          INSERT (ticker, symbol, date, price, volume, marketCap, peRatioTTM, psRatioTTM, pbRatioTTM, enterpriseValue, isInternational, lastUpdated)
          VALUES (S.ticker, S.symbol, S.date, S.price, S.volume, S.marketCap, S.peRatioTTM, S.psRatioTTM, S.pbRatioTTM, S.enterpriseValue, S.isInternational, S.lastUpdated)
      `;

      await this.bigquery.query({ query: mergeQuery });
      console.log("Merged temp price metrics table into main table successfully.");
    } catch (error) {
      console.error("Error merging temp price metrics into main table:", error);
      throw error;
    } finally {
      try {
        const dropTempTableQuery = `
          DROP TABLE IF EXISTS \`${this.dataset}.${this.tempTable}\`
        `;
        await this.bigquery.query({ query: dropTempTableQuery });
        console.log("Dropped temporary price metrics table successfully.");
      } catch (dropError) {
        console.error("Error dropping temp table:", dropError);
      }
    }
  }
}

export class FinancialsDataManager {
  private bigquery: BigQuery;
  private dataset: string;
  private table: string;
  private tempTable: string;
  static readonly quarterlyTableId = "quarterly";
  static readonly annualTableId = "annual";
  static readonly datasetId = "financials";

  constructor(table: string) {
    const credentials = this.setupCredentials();
    this.bigquery = new BigQuery({ credentials });
    this.dataset = FinancialsDataManager.datasetId;
    this.table = table;
    this.tempTable = `${table}_temp_${Date.now()}`;
  }

  private setupCredentials() {
    const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!credentialsJson) {
      throw new Error(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set"
      );
    }
    try {
      return JSON.parse(credentialsJson);
    } catch (error) {
      throw new Error("Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    }
  }

  static async createQuarterlyTable(schema: any[]) {
    const instance = new FinancialsDataManager(this.quarterlyTableId);
    await instance.createTableIfNotExists(schema);
    await instance.createTempTable(schema);
    return instance;
  }

  static async createAnnualTable(schema: any[]) {
    const instance = new FinancialsDataManager(this.annualTableId);
    await instance.createTableIfNotExists(schema);
    await instance.createTempTable(schema);
    return instance;
  }

  private async createTable(tableName: string, schema?: any[]) {
    try {
      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(tableName);

      let [exists] = await table.exists();
      let iterations = 0;

      while (!exists && iterations < 3) {
        if (iterations === 0 && schema) {
          await table.create({ schema });
          console.log(`Created table ${tableName} with schema`);
        }
        iterations++;
        [exists] = await table.exists();
        await new Promise((resolve) =>
          setTimeout(resolve, 2 ** iterations * 1000)
        );
      }
    } catch (error) {
      console.error(`Error creating table ${tableName}:`, error);
      throw error;
    }
  }

  private async createTableIfNotExists(schema: any[]) {
    await this.createTable(this.table, schema);
  }

  private async createTempTable(schema: any[]) {
    await this.createTable(this.tempTable, schema);
  }

  private filterNumericFields(data: any) {
    const requiredFields = ["ticker", "symbol", "date"];
    const filtered: any = {};

    Object.entries(data).forEach(([key, value]) => {
      if (requiredFields.includes(key)) {
        filtered[key] = value;
      } else if (typeof value === "number" || !isNaN(Number(value))) {
        // Only include numeric values
        filtered[key] = Number(value);
      }
    });

    return filtered;
  }

  async insertFinancialsToTempTable(financials: any[]): Promise<void> {
    try {
      if (!financials.length) {
        console.log("No financial data to upsert");
        return;
      }

      const dataset = this.bigquery.dataset(this.dataset);
      const table = dataset.table(this.tempTable);

      const rows = financials.map((item) => ({
        ...this.filterNumericFields(item),
        date: BigQuery.timestamp(
          moment(item.date).format("YYYY-MM-DD HH:mm:ss.SSSZ")
        ),
      }));

      const batchSize = 500;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        await table.insert(batch);
        console.log(`Inserted batch of ${batch.length} financial rows.`);
      }
    } catch (error) {
      console.error("Error inserting financials to temp table:", error);
      throw error;
    }
  }

  async mergeTempTableToMainTable(): Promise<void> {
    try {
      // Get column names from a sample row
      const [sampleRow] = await this.bigquery.query({
        query: `SELECT * FROM \`${this.dataset}.${this.tempTable}\` LIMIT 1`,
      });

      const columns = Object.keys(sampleRow[0] || {});
      const columnList = columns.join(", ");

      const mergeQuery = `
          MERGE \`${this.dataset}.${this.table}\` T
          USING \`${this.dataset}.${this.tempTable}\` S
          ON T.ticker = S.ticker AND T.date = S.date
          WHEN MATCHED THEN
            UPDATE SET ${columns.map((col) => `T.${col} = S.${col}`).join(", ")}
          WHEN NOT MATCHED THEN
            INSERT (${columnList})
            VALUES (${columns.map((col) => `S.${col}`).join(", ")})
        `;

      await this.bigquery.query({ query: mergeQuery });
      console.log("Merged temp financials table into main table successfully.");
    } catch (error) {
      console.error("Error merging temp financials into main table:", error);
      throw error;
    } finally {
      // Always attempt to drop the temp table
      try {
        const dropTempTableQuery = `
            DROP TABLE IF EXISTS \`${this.dataset}.${this.tempTable}\`
          `;
        await this.bigquery.query({ query: dropTempTableQuery });
        console.log("Dropped temporary financials table successfully.");
      } catch (dropError) {
        console.error("Error dropping temp table:", dropError);
      }
    }
  }
}

