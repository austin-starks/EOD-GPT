import axios, { AxiosInstance } from "axios";

type Year = `${number}${number}${number}${number}`;
type Month = `${number}${number}`;
type Day = `${number}${number}`;
type Datestring = `${Year}-${Month}-${Day}`;

interface FinancialStatement {
  currency_symbol?: string;
  quarterly_last_0?: Record<string, any>;
  quarterly_last_1?: Record<string, any>;
  quarterly_last_2?: Record<string, any>;
  quarterly_last_3?: Record<string, any>;
  yearly_last_0?: Record<string, any>;
  yearly_last_1?: Record<string, any>;
  yearly_last_2?: Record<string, any>;
  yearly_last_3?: Record<string, any>;
}

interface HistoricalFinancialStatement {
  quarterly?: Record<Datestring, any>;
  yearly?: Record<Datestring, any>;
  currency_symbol?: string;
}

export interface EodhdBulkFundamentalsResponse {
  ETF_Data?: Record<string, any>;
  General?: {
    Address?: string;
    Code?: string;
    CUSIP?: string;
    Type?: string;
    Name?: string;
    Exchange?: string;
    CurrencyCode?: string;
  };
  Highlights?: {
    MarketCapitalization?: number;
    [key: string]: any;
  };
  Valuation?: Record<string, any>;
  SharesStats?: Record<string, any>;
  Technicals?: Record<string, any>;
  SplitsDividends?: Record<string, any>;
  AnalystRatings?: Record<string, any>;
  Earnings?: Record<string, any>;
  Financials?: {
    Balance_Sheet?: FinancialStatement;
    Cash_Flow?: FinancialStatement;
    Income_Statement?: FinancialStatement;
  };
}

export interface EodhdFundamentalsResponse {
  ETF_Data?: {
    Holdings?: Record<string, { Code: string; "Assets_%": number }>;
  };
  General?: {
    Code?: string;
    CUSIP?: string;
    Type?: string;
    Name?: string;
    Exchange?: string;
    CurrencyCode?: string;
    Address?: string;
    AddressData?: {
      Street?: string;
      City?: string;
      State?: string;
      Country?: string;
      ZIP?: string;
    };
    Description?: string;
    IPODate?: string;
    Sector?: string;
    Industry?: string;
  };
  Earnings?: {
    History?: any[];
    Annual?: any[];
  };
  Financials?: {
    Balance_Sheet?: HistoricalFinancialStatement;
    Cash_Flow?: HistoricalFinancialStatement;
    Income_Statement?: HistoricalFinancialStatement;
  };
}

export interface EodhdHistoricalPriceData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  adjusted_close: number;
  volume: number;
}

export interface EodhdRealTimePriceData {
  code: string;
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  previousClose: number;
  change: number;
  change_p: number;
}

export class EodhdClient {
  private readonly baseUrl: string = "https://eodhd.com/api";
  private readonly apiToken: string;
  private readonly client: AxiosInstance;

  constructor(apiToken: string = process.env.EOD_API_TOKEN!) {
    this.apiToken = apiToken;
    this.client = axios.create({
      baseURL: this.baseUrl,
      params: {
        api_token: this.apiToken,
        fmt: "json",
      },
    });
  }

  private buildUrl(endpoint: string): string {
    return `${endpoint}`;
  }

  async getFundamentals(ticker: string, exchange: string = "US"): Promise<EodhdFundamentalsResponse> {
    try {
      const url = this.buildUrl(`/fundamentals/${ticker}.${exchange}`);
      const { data } = await this.client.get(url);
      return data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(`Failed to fetch data for ${ticker}.${exchange}: ${error.message}`);
      }
      throw error;
    }
  }

  async getBulkFundamentals(
    exchange: string,
    symbols: string[],
    offset?: number,
    limit: number = 500
  ): Promise<EodhdBulkFundamentalsResponse[]> {
    try {
      const symbolsParam = symbols.map((s) => `${s}.${exchange}`).join(",");
      const url = this.buildUrl(`/bulk-fundamentals/${exchange}`);
      const { data } = await this.client.get(url, {
        params: {
          symbols: symbolsParam,
          offset,
          limit,
          version: "1.2",
        },
      });
      console.log(data);
      return data;
    } catch (error) {
      console.error(error);
      throw error;
    }
  }

  async getHistoricalPriceData(
    ticker: string,
    exchange: string = "US",
    from: string,
    to: string,
    period: "d" | "w" | "m" = "d"
  ): Promise<EodhdHistoricalPriceData[]> {
    try {
      const url = this.buildUrl(`/eod/${ticker}.${exchange}`);
      const { data } = await this.client.get(url, {
        params: {
          from,
          to,
          period,
        },
      });
      return data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(`Failed to fetch historical price data for ${ticker}.${exchange}: ${error.message}`);
      }
      throw error;
    }
  }

  async getRealTimePrice(
    ticker: string,
    exchange: string = "US"
  ): Promise<EodhdRealTimePriceData> {
    try {
      const url = this.buildUrl(`/real-time/${ticker}.${exchange}`);
      const { data } = await this.client.get(url, {
        params: {
          fmt: "json",
        },
      });
      return data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(`Failed to fetch real-time price for ${ticker}.${exchange}: ${error.message}`);
      }
      throw error;
    }
  }

  async getExchangeSymbols(exchange: string): Promise<any[]> {
    try {
      const url = this.buildUrl(`/exchange-symbol-list/${exchange}`);
      const { data } = await this.client.get(url);
      return data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(`Failed to fetch symbols for exchange ${exchange}: ${error.message}`);
      }
      throw error;
    }
  }
}
