# Financial Data Downloader

This project downloads financial data (quarterly and annual) for a given stock ticker from EOD Historical Data and stores it in both MongoDB and Google BigQuery.

## Prerequisites

Before you begin, ensure you have the following:

- **Node.js** (version 18 or higher) and **npm** installed.
- **MongoDB** installed and running locally or accessible via a connection string.
- **Google Cloud Platform (GCP) account** with BigQuery enabled.
- An **EOD Historical Data API key**. You can sign up for a free or paid plan at [https://eodhd.com/r?ref=nexustrade](https://eodhd.com/r?ref=nexustrade). Using this link helps support the project!
- **Requesty API key**. You can sign up for Requesty at [https://app.requesty.ai/join?ref=e0603ee5](https://app.requesty.ai/join?ref=e0603ee5). Using this referral link helps support the project!
- A `.env` file in the root directory with the following variables:

  ```
  CLOUD_DB="mongodb://localhost:27017/your_cloud_db" # Replace with your MongoDB connection string
  LOCAL_DB="mongodb://localhost:27017/your_local_db" # Replace with your MongoDB connection string
  EODHD_API_KEY="YOUR_EODHD_API_KEY" # Replace with your EODHD API key
  REQUESTY_API_KEY="YOUR_REQUESTY_API_KEY" # Replace with your Requesty API key
  GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}' # Replace with your GCP service account credentials JSON
  ```

  **Important:** The `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable should contain the _entire_ JSON content of your Google Cloud service account key. This is necessary for authenticating with BigQuery. Make sure this is properly formatted and secured.

## Setup

1.  **Clone the repository:**

    ```bash
    git clone <your_repository_url>
    cd <your_repository_directory>
    ```

2.  **Install dependencies:**

    ```bash
    npm install
    ```

## Configuration

1.  **Create a `.env` file** in the root directory of the project. Populate it with the necessary environment variables as described in the "Prerequisites" section. **Do not commit this file to your repository!**

2.  **Set up Google Cloud credentials:**

    - Create a Google Cloud service account with BigQuery Data Editor permissions.
    - Download the service account key as a JSON file.
    - Set the `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable to the contents of this file. **Ensure proper JSON formatting.**

## Running the Script

You have two options for running the script:

**Option 1: Using `node` directly (requires compilation)**

1.  **Compile the TypeScript code:**

    ```bash
    npm run build
    ```

    This will create a `dist` directory with the compiled JavaScript files.

2.  **Run the compiled script:**

    ```bash
    node dist/index.js
    ```

**Option 2: Using `ts-node` (for development/easier execution)**

1.  **Install `ts-node` globally (if you haven't already):**

    ```bash
    npm install -g ts-node
    ```

2.  **Run the script directly:**

    ```bash
    ts-node index.ts
    ```

## Usage

To download financial data for a specific stock ticker, modify the `index.ts` file to specify the ticker you want to process. For example:

```typescript
// In index.ts
(async () => {
  try {
    const cloud = new Database("local");
    await cloud.connect();
    console.log("Connected to database");
    const startTime = new Date();
    const processor = new EarningsProcessor();
    await processor.processAndSaveEarnings("MSFT"); // Changed from AAPL to MSFT
    const endTime = new Date();
    const duration = endTime.getTime() - startTime.getTime();
    console.log(
      Processing complete. Files have been saved. Time taken: ${duration}ms
    );
  } catch (error) {
    console.error("Error:", error);
  } finally {
    process.exit(0);
  }
})();
```

Then, run the script using either of the methods described above.

## Important Considerations

- **Error Handling:** The script includes basic error handling, but you may want to enhance it for production use. Consider adding more robust logging and retry mechanisms.
- **Rate Limiting:** Be mindful of the EOD Historical Data API's rate limits. Implement appropriate delays or batching to avoid exceeding the limits.
- **Data Validation:** The script filters numeric fields before inserting into BigQuery. You may want to add more comprehensive data validation to ensure data quality.
- **BigQuery Costs:** Be aware of BigQuery's pricing model. Storing and querying large datasets can incur costs. Optimize your queries and data storage strategies to minimize expenses.
- **MongoDB Connection:** Ensure your MongoDB instance is running and accessible from the machine running the script.
- **Security:** Protect your API keys and service account credentials. Do not hardcode them in your code or commit them to your repository. Use environment variables and secure storage mechanisms.

## Contributing

Contributions are welcome! Please submit a pull request with your changes.

## License

[MIT License](LICENSE) (Replace with your chosen license)
