import RequestyServiceClient, {
  RequestyModelEnum,
} from "./helpers/RequestyServiceClient";

import Database from "./helpers/Db";
import { FinancialsDataManager } from "./helpers/bigQuery";

class QueryProcessor {
  private requestyClient: RequestyServiceClient;
  private financialsManager: FinancialsDataManager;

  constructor() {
    this.requestyClient = new RequestyServiceClient();
    this.financialsManager = new FinancialsDataManager(
      FinancialsDataManager.quarterlyTableId
    );
  }

  async processNaturalLanguageQuery(question: string) {
    // 1. Convert natural language to SQL using Requesty
    const systemPrompt = `You are an expert at writing BigQuery SQL queries.
    
# Financials Table Schema (\`financials.quarterly\` or \`financials.annual\`)
[
  {
    "name": "ticker",
    "type": "STRING"
  },
  {
    "name": "symbol",
    "type": "STRING"
  },
  {
    "name": "date",
    "type": "TIMESTAMP"
  },
  {
    "name": "accountsPayable",
    "type": "FLOAT"
  },
  {
    "name": "accumulatedOtherComprehensiveIncome",
    "type": "FLOAT"
  },
  {
    "name": "beginPeriodCashFlow",
    "type": "FLOAT"
  },
  {
    "name": "capitalExpenditures",
    "type": "FLOAT"
  },
  {
    "name": "capitalStock",
    "type": "FLOAT"
  },
  {
    "name": "cash",
    "type": "FLOAT"
  },
  {
    "name": "cashAndShortTermInvestments",
    "type": "FLOAT"
  },
  {
    "name": "changeInCash",
    "type": "FLOAT"
  },
  {
    "name": "changeInWorkingCapital",
    "type": "FLOAT"
  },
  {
    "name": "changeToAccountReceivables",
    "type": "FLOAT"
  },
  {
    "name": "changeToInventory",
    "type": "FLOAT"
  },
  {
    "name": "commonStock",
    "type": "FLOAT"
  },
  {
    "name": "commonStockSharesOutstanding",
    "type": "FLOAT"
  },
  {
    "name": "costOfRevenue",
    "type": "FLOAT"
  },
  {
    "name": "currentDeferredRevenue",
    "type": "FLOAT"
  },
  {
    "name": "depreciation",
    "type": "FLOAT"
  },
  {
    "name": "dividendsPaid",
    "type": "FLOAT"
  },
  {
    "name": "ebitda",
    "type": "FLOAT"
  },
  {
    "name": "endPeriodCashFlow",
    "type": "FLOAT"
  },
  {
    "name": "freeCashFlow",
    "type": "FLOAT"
  },
  {
    "name": "grossProfit",
    "type": "FLOAT"
  },
  {
    "name": "incomeBeforeTax",
    "type": "FLOAT"
  },
  {
    "name": "incomeTaxExpense",
    "type": "FLOAT"
  },
  {
    "name": "inventory",
    "type": "FLOAT"
  },
  {
    "name": "investments",
    "type": "FLOAT"
  },
  {
    "name": "liabilitiesAndStockholdersEquity",
    "type": "FLOAT"
  },
  {
    "name": "longTermDebt",
    "type": "FLOAT"
  },
  {
    "name": "longTermInvestments",
    "type": "FLOAT"
  },
  {
    "name": "netDebt",
    "type": "FLOAT"
  },
  {
    "name": "netIncome",
    "type": "FLOAT"
  },
  {
    "name": "netIncomeFromContinuingOps",
    "type": "FLOAT"
  },
  {
    "name": "netInvestedCapital",
    "type": "FLOAT"
  },
  {
    "name": "netReceivables",
    "type": "FLOAT"
  },
  {
    "name": "netWorkingCapital",
    "type": "FLOAT"
  },
  {
    "name": "nonCurrentAssetsTotal",
    "type": "FLOAT"
  },
  {
    "name": "nonCurrentLiabilitiesOther",
    "type": "FLOAT"
  },
  {
    "name": "nonCurrentLiabilitiesTotal",
    "type": "FLOAT"
  },
  {
    "name": "nonCurrrentAssetsOther",
    "type": "FLOAT"
  },
  {
    "name": "operatingIncome",
    "type": "FLOAT"
  },
  {
    "name": "otherCashflowsFromFinancingActivities",
    "type": "FLOAT"
  },
  {
    "name": "otherCashflowsFromInvestingActivities",
    "type": "FLOAT"
  },
  {
    "name": "otherCurrentAssets",
    "type": "FLOAT"
  },
  {
    "name": "otherCurrentLiab",
    "type": "FLOAT"
  },
  {
    "name": "otherNonCashItems",
    "type": "FLOAT"
  },
  {
    "name": "otherOperatingExpenses",
    "type": "FLOAT"
  },
  {
    "name": "propertyPlantAndEquipmentGross",
    "type": "FLOAT"
  },
  {
    "name": "propertyPlantAndEquipmentNet",
    "type": "FLOAT"
  },
  {
    "name": "reconciledDepreciation",
    "type": "FLOAT"
  },
  {
    "name": "researchDevelopment",
    "type": "FLOAT"
  },
  {
    "name": "retainedEarnings",
    "type": "FLOAT"
  },
  {
    "name": "salePurchaseOfStock",
    "type": "FLOAT"
  },
  {
    "name": "sellingGeneralAdministrative",
    "type": "FLOAT"
  },
  {
    "name": "shortLongTermDebt",
    "type": "FLOAT"
  },
  {
    "name": "shortLongTermDebtTotal",
    "type": "FLOAT"
  },
  {
    "name": "shortTermDebt",
    "type": "FLOAT"
  },
  {
    "name": "shortTermInvestments",
    "type": "FLOAT"
  },
  {
    "name": "stockBasedCompensation",
    "type": "FLOAT"
  },
  {
    "name": "taxProvision",
    "type": "FLOAT"
  },
  {
    "name": "totalAssets",
    "type": "FLOAT"
  },
  {
    "name": "totalCashFromFinancingActivities",
    "type": "FLOAT"
  },
  {
    "name": "totalCashFromOperatingActivities",
    "type": "FLOAT"
  },
  {
    "name": "totalCurrentAssets",
    "type": "FLOAT"
  },
  {
    "name": "totalCurrentLiabilities",
    "type": "FLOAT"
  },
  {
    "name": "totalLiab",
    "type": "FLOAT"
  },
  {
    "name": "totalOperatingExpenses",
    "type": "FLOAT"
  },
  {
    "name": "totalOtherIncomeExpenseNet",
    "type": "FLOAT"
  },
  {
    "name": "totalRevenue",
    "type": "FLOAT"
  },
  {
    "name": "totalStockholderEquity",
    "type": "FLOAT"
  },
  {
    "name": "depreciationAndAmortization",
    "type": "FLOAT"
  },
  {
    "name": "ebit",
    "type": "FLOAT"
  },
  {
    "name": "otherStockholderEquity",
    "type": "FLOAT"
  },
  {
    "name": "interestExpense",
    "type": "FLOAT"
  },
  {
    "name": "capitalLeaseObligations",
    "type": "FLOAT"
  },
  {
    "name": "capitalSurpluse",
    "type": "FLOAT"
  },
  {
    "name": "cashAndCashEquivalentsChanges",
    "type": "FLOAT"
  },
  {
    "name": "cashAndEquivalents",
    "type": "FLOAT"
  },
  {
    "name": "changeReceivables",
    "type": "FLOAT"
  },
  {
    "name": "interestIncome",
    "type": "FLOAT"
  },
  {
    "name": "longTermDebtTotal",
    "type": "FLOAT"
  },
  {
    "name": "netIncomeApplicableToCommonShares",
    "type": "FLOAT"
  },
  {
    "name": "netInterestIncome",
    "type": "FLOAT"
  },
  {
    "name": "nonOperatingIncomeNetOther",
    "type": "FLOAT"
  },
  {
    "name": "otherAssets",
    "type": "FLOAT"
  },
  {
    "name": "propertyPlantEquipment",
    "type": "FLOAT"
  },
  {
    "name": "totalCashflowsFromInvestingActivities",
    "type": "FLOAT"
  },
  {
    "name": "accumulatedDepreciation",
    "type": "FLOAT"
  },
  {
    "name": "cashFlowsOtherOperating",
    "type": "FLOAT"
  },
  {
    "name": "changeToLiabilities",
    "type": "FLOAT"
  },
  {
    "name": "changeToNetincome",
    "type": "FLOAT"
  },
  {
    "name": "changeToOperatingActivities",
    "type": "FLOAT"
  },
  {
    "name": "commonStockTotalEquity",
    "type": "FLOAT"
  },
  {
    "name": "netBorrowings",
    "type": "FLOAT"
  },
  {
    "name": "netTangibleAssets",
    "type": "FLOAT"
  },
  {
    "name": "otherLiab",
    "type": "FLOAT"
  },
  {
    "name": "retainedEarningsTotalEquity",
    "type": "FLOAT"
  },
  {
    "name": "issuanceOfCapitalStock",
    "type": "FLOAT"
  },
  {
    "name": "additionalPaidInCapital",
    "type": "FLOAT"
  },
  {
    "name": "deferredLongTermLiab",
    "type": "FLOAT"
  },
  {
    "name": "discontinuedOperations",
    "type": "FLOAT"
  },
  {
    "name": "effectOfAccountingCharges",
    "type": "FLOAT"
  },
  {
    "name": "extraordinaryItems",
    "type": "FLOAT"
  },
  {
    "name": "goodWill",
    "type": "FLOAT"
  },
  {
    "name": "minorityInterest",
    "type": "FLOAT"
  },
  {
    "name": "nonRecurring",
    "type": "FLOAT"
  },
  {
    "name": "noncontrollingInterestInConsolidatedEntity",
    "type": "FLOAT"
  },
  {
    "name": "otherItems",
    "type": "FLOAT"
  },
  {
    "name": "preferredStockTotalEquity",
    "type": "FLOAT"
  },
  {
    "name": "temporaryEquityRedeemableNoncontrollingInterests",
    "type": "FLOAT"
  },
  {
    "name": "totalPermanentEquity",
    "type": "FLOAT"
  },
  {
    "name": "treasuryStock",
    "type": "FLOAT"
  },
  {
    "name": "intangibleAssets",
    "type": "FLOAT"
  },
  {
    "name": "sellingAndMarketingExpenses",
    "type": "FLOAT"
  },
  {
    "name": "warrants",
    "type": "FLOAT"
  },
  {
    "name": "accumulatedAmortization",
    "type": "FLOAT"
  },
  {
    "name": "deferredLongTermAssetCharges",
    "type": "FLOAT"
  },
  {
    "name": "exchangeRateChanges",
    "type": "FLOAT"
  },
  {
    "name": "negativeGoodwill",
    "type": "FLOAT"
  },
  {
    "name": "preferredStockAndOtherAdjustments",
    "type": "FLOAT"
  },
  {
    "name": "preferredStockRedeemable",
    "type": "FLOAT"
  },
  {
    "name": "earningAssets",
    "type": "FLOAT"
  }
]

The database has a table called 'financials.quarterly' and 'financials.annual' with the above columns.
Respond only with the SQL query, no explanation. You are allowed to write comments in the query.
The query should be valid BigQuery SQL.

# CONSTRAINTS:
- The default limit of results is 25. The maximum is 100. If they ask for more than 100, use the limit.
- Always include the date and raw values in the response.

# RESPONSE FORMAT:
\`\`\`sql
-- Comment (if needed)
SELECT * FROM \`financials.quarterly\` -- This table might be annual or quarterly. There might be where clauses, joins, or whatever is needed to answer the question.
\`\`\`

# Examples:

// Example 1:
User: "What stocks have the highest revenue?"
Assistant:
\`\`\`sql
-- Query to retrieve stocks with the highest revenue from each stock's latest annual report
WITH LatestAnnual AS (
  SELECT
    date,
    ticker,
    symbol,
    totalRevenue,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
  FROM \`financials.annual\`
)
SELECT
  date,
  ticker,
  symbol,
  totalRevenue
FROM LatestAnnual
WHERE rn = 1
ORDER BY totalRevenue DESC
LIMIT 10;
\`\`\`

// Example 2:
User: "What stocks have increased their free cash flow every quarter for the last 4 quarters? Sort by their current free cash flow as of the last quarter descending"
Assistant:
\`\`\`sql
-- Query to retrieve stocks with increasing free cash flow over the last 4 quarters, sorted by the most recent quarter's free cash flow in descending order
WITH Last4Quarters AS (
  SELECT
    ticker,
    symbol,
    freeCashFlow,
    date,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
  FROM \`financials.quarterly\`
)
SELECT
  t1.date,
  t1.ticker,
  t1.symbol,
  t1.freeCashFlow AS current_freeCashFlow
FROM Last4Quarters t1
JOIN Last4Quarters t2 ON t1.ticker = t2.ticker AND t2.rn = 2
JOIN Last4Quarters t3 ON t1.ticker = t3.ticker AND t3.rn = 3
JOIN Last4Quarters t4 ON t1.ticker = t4.ticker AND t4.rn = 4
WHERE t4.freeCashFlow < t3.freeCashFlow
  AND t3.freeCashFlow < t2.freeCashFlow
  AND t2.freeCashFlow < t1.freeCashFlow
ORDER BY t1.freeCashFlow DESC
LIMIT 25;
\`\`\`
    `;
    console.log("Sending Request to Requesty");
    const response = await this.requestyClient.sendRequest({
      systemPrompt,
      model: RequestyModelEnum.geminiFlash2,
      temperature: 1,
      messages: [
        {
          sender: "User",
          content: `Convert this question to a SQL query: ${question}`,
        },
      ],
      userId: "system",
    });

    const sqlQuery = response.choices[0].message?.content
      ?.trim()
      .replace(/^```sql\n?/, "") // Remove opening ```sql
      .replace(/^```\n?/, "") // Remove opening ``` (if no sql)
      .replace(/\n?```$/, ""); // Remove closing ```
    if (!sqlQuery) {
      throw new Error("No SQL query generated");
    }
    console.log("Generated SQL Query:", sqlQuery);

    // 3. Execute the query against BigQuery
    try {
      const results = await this.financialsManager.executeQuery(sqlQuery);
      return results;
    } catch (error) {
      console.error("Error executing query:", error);
      throw error;
    }
  }
}

(async () => {
  try {
    const db = new Database("local");
    await db.connect();
    const queryProcessor = new QueryProcessor();
    const results = await queryProcessor.processNaturalLanguageQuery(
      "What stocks have the highest income (annual) right now?"
    );

    console.log("Query Results:", results);
  } catch (error) {
    console.error("Error:", error);
  } finally {
    process.exit(0);
  }
})();
