import { Document, Schema, model } from "mongoose";

export interface IInternationalStock extends Document {
  ticker: string;
  symbol: string;
  country: string;
  commonStockSharesOutstanding: number;
  date: Date;
  name?: string;
}

const InternationalStockSchema = new Schema<IInternationalStock>(
  {
    ticker: { type: String, required: true },
    symbol: { type: String, required: true },
    country: { type: String, required: true },
    commonStockSharesOutstanding: { type: Number, default: 0 },
    date: { type: Date, required: true },
    name: { type: String, required: false },
  },
  {
    timestamps: true,
  }
);

// Create compound index for efficient lookups
InternationalStockSchema.index({ ticker: 1, date: -1 }, { unique: true });
InternationalStockSchema.index({ country: 1 });

const InternationalStock = model<IInternationalStock>(
  "InternationalStock",
  InternationalStockSchema
);

export default InternationalStock;
