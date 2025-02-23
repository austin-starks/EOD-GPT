import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import mongoose from "mongoose";

dotenv.config({ path: ".env" });

const cloudDB = process.env.CLOUD_DB;
const localDB = process.env.LOCAL_DB;
const connectionMap = new Map();
connectionMap.set("cloudDB", cloudDB);
connectionMap.set("cloud", cloudDB);
connectionMap.set("localDB", localDB);
connectionMap.set("local", localDB);

mongoose.set("strictQuery", false);

class Database {
  private dbType: string;
  private testDbInstance: any;
  private mongoClient: MongoClient | null = null;

  constructor(dbType: "local" | "cloud" | "test") {
    if (!dbType) throw new Error("No database type provided");
    this.dbType = dbType;
  }

  getMongoClient() {
    return this.mongoClient;
  }

  private async connectHelper() {
    const connectionURL = connectionMap.get(this.dbType);
    await mongoose.connect(connectionURL);
    this.mongoClient = new MongoClient(connectionURL);
    await this.mongoClient.connect();
    console.log(
      "Successfully connected to " + this.dbType + " database server!"
    );
  }

  private async connectTest() {
    const { MongoMemoryServer } = require("mongodb-memory-server");
    this.testDbInstance = new MongoMemoryServer();
    await this.testDbInstance.start();
    const uri = this.testDbInstance.getUri();
    await mongoose.connect(uri);
  }

  public async connect() {
    if (this.dbType === "cloud" || this.dbType === "local") {
      await this.connectHelper();
    } else if (this.dbType === "test") {
      await this.connectTest();
    }
  }

  public async disconnect() {
    if (this.dbType === "cloud" || this.dbType === "local") {
      if (this.mongoClient) await this.mongoClient.close();
      await mongoose.connection.close();
    } else if (this.dbType === "test") {
      await mongoose.connection.dropDatabase();
      await mongoose.connection.close();
      await this.testDbInstance.stop();
    }
  }

  public async clearDatabase() {
    if (!this.dbType || this.dbType === "cloud" || this.dbType === "local") {
      throw new Error("Not Implemented");
    } else if (this.dbType === "test") {
      const collections = mongoose.connection.collections;
      for (const key in collections) {
        const collection = collections[key];
        await collection.deleteMany({});
      }
    }
  }

  public async killLongRunningQueries(thresholdSeconds: number) {
    if (!this.mongoClient) {
      throw new Error("MongoClient is not connected");
    }

    const adminDb = this.mongoClient.db().admin();
    const currentOps = await adminDb.command({ currentOp: 1 });

    const longRunningOps = currentOps.inprog.filter((op: any) => {
      return op.secs_running > thresholdSeconds && op.op === "query";
    });

    for (const op of longRunningOps) {
      try {
        await adminDb.command({ killOp: 1, op: op.opid });
        console.log(`Killed long-running operation: ${op.opid}`);
      } catch (error) {
        console.error(`Failed to kill operation ${op.opid}: ${error.message}`);
      }
    }

    console.log(
      `Checked for long-running queries exceeding ${thresholdSeconds} seconds.`
    );
  }

  public async getLongRunningQueriesDetails(thresholdSeconds: number) {
    if (!this.mongoClient) {
      throw new Error("MongoClient is not connected");
    }

    const adminDb = this.mongoClient.db().admin();
    const currentOps = await adminDb.command({ currentOp: 1 });

    const longRunningOps = currentOps.inprog.filter((op: any) => {
      return op.secs_running > thresholdSeconds && op.op === "query";
    });

    return longRunningOps.map((op: any) => ({
      operationId: op.opid,
      namespace: op.ns,
      description: op.desc,
      client: op.client,
      appName: op.appName,
      duration: op.secs_running,
      planSummary: op.planSummary,
      query: op.command,
      lockStats: op.lockStats,
      waitingForLock: op.waitingForLock,
      numYields: op.numYields,
      threadId: op.threadId,
    }));
  }
}

export default Database;
