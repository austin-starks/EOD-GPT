# Financial Data Downloader & AI Query System

This project downloads financial data (quarterly and annual) for stocks from EOD Historical Data and stores it in both MongoDB and Google BigQuery. It also includes an AI-powered natural language interface for querying the financial data.

## Prerequisites

Before you begin, ensure you have the following:

- **Node.js** (version 18 or higher) and **npm** installed.
- **MongoDB** installed and running locally or accessible via a connection string.
- **Google Cloud Platform (GCP) account** with BigQuery enabled.
- An **EOD Historical Data API key** ([Sign up for a free or paid plan here](https://eodhd.com/r?ref=nexustrade) - referral link).
- **Requesty API key** ([Sign up for Requesty here](https://app.requesty.ai/join?ref=e0603ee5) - referral link).
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
    git clone https://github.com/austin-starks/FinAnGPT-Pro
    cd FinAnGPT-Pro
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

### Downloading Financial Data

1. **For all stocks in your watchlist:**

   - The project includes a `tickers.csv` file with a pre-populated list of major US stocks
   - You can modify this file to add or remove tickers (one ticker per line, skip the header row)
   - Run the upload script:

   ```bash
   ts-node upload.ts
   ```

2. **For a single stock:**
   - Modify the `upload.ts` script to use `processAndSaveEarningsForOneStock`:
   ```typescript
   // In upload.ts
   const processor = new EarningsProcessor();
   await processor.processAndSaveEarningsForOneStock("AAPL"); // Replace with your desired ticker
   ```

### File Structure

```
.
├── src/
├── tickers.csv     # Pre-populated list of major US stocks
├── .env            # Your environment variables (create this)
└── ... other files
```

### Querying Financial Data with AI

The project includes a natural language interface for querying financial data. You can ask questions in plain English about the stored financial data.

To use the AI query system:

1. **Run the chat script:**

   ```bash
   ts-node chat.ts
   ```

2. **Example queries you can try:**
   - "What stocks have the highest revenue?"
   - "Show me companies with increasing free cash flow over the last 4 quarters"
   - "Which companies have the highest net income in their latest annual report?"
   - "List the top 10 companies by EBITDA margin"

The system will convert your natural language query into SQL, execute it against BigQuery, and return the results.

### Example Output

Here's a sample response when asking about companies with the highest net income:

```
Here's a summary of the stocks with the highest reported net income:

**Summary:**
The top companies by net income are primarily in the technology and finance sectors. Alphabet (GOOG/GOOGL) leads, followed by Berkshire Hathaway (BRK-A/BRK-B), Apple (AAPL), and Microsoft (MSFT).

**Top Stocks by Net Income:**

| Ticker | Symbol | Net Income (USD) | Date       |
|--------|--------|------------------|------------|
| GOOG   | GOOG   | 100,118,000,000 | 2025-02-05 |
| GOOGL  | GOOGL  | 100,118,000,000 | 2025-02-05 |
| BRK-B  | BRK-B  | 96,223,000,000  | 2024-02-26 |
| BRK-A  | BRK-A  | 96,223,000,000  | 2024-02-26 |
| AAPL   | AAPL   | 93,736,000,000  | 2024-11-01 |

**Insights:**
- The data includes both Class A and Class B shares for Alphabet and Berkshire Hathaway
- Most recent data is from early 2025 reporting period
```

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
