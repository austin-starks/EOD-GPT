import { BigQuery } from "@google-cloud/bigquery";
import moment from "moment";

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
