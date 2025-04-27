import pandas as pd
from pandas import DataFrame
from enum import Enum
from typing import List
import os

class HistoryType(Enum):
    Order = 1
    Dividend = 2
    Transaction = 3
    Interest = 4

class TransactionType(Enum):
    MarketBuy = 'Market buy'
    MarketSell = 'Market sell'
    LimitBuy = 'Limit buy'
    LimitSell = 'Limit sell'
    StopSell = 'Stop sell'
    StopBuy = 'Stop buy'
    StopLimitBuy = 'Stop limit buy'
    StopLimitSell = 'Stop limit sell'
class ActionType(Enum):
    Deposit = 'Deposit'
    Withdrawal = 'Withdrawal'
    Dividend = 'Dividend (Dividend)'
    InterestOnCash = 'Interest on cash'
    MarketBuy = 'Market buy'
    MarketSell = 'Market sell'
    LimitBuy = 'Limit buy'
    LimitSell = 'Limit sell'
    StopSell = 'Stop sell'
    StopBuy = 'Stop buy'
    StopLimitBuy = 'Stop limit buy'
    StopLimitSell = 'Stop limit sell'

def read_csv(path: str) -> DataFrame:
    print(f"Reading CSV file from {path}")
    df = pd.read_csv(path, header=0)
    _validate(df, [ActionTypeValidator(), DateValidator()])
    return df

class Validator:
    def validate(self, df: DataFrame): 
        pass
class ActionTypeValidator(Validator):
    def validate(self, df: DataFrame):
        actual = set(df['Action'].unique())
        expected = set(map(lambda x: x.value, ActionType._member_map_.values()))
        for a in actual:
            if a not in expected:
                raise ValueError(f"Invalid action type: {a}, should be within {expected}")
        print("All action types are valid.")
class DateValidator(Validator):
    def validate(self, df: DataFrame):
        if 'Time' not in df.columns:
            raise ValueError("Time column is missing.")
        try:
            df['Time'] = pd.to_datetime(df['Time'], format='mixed')
        except Exception as e:
            raise ValueError(f"Invalid date format: {e}")
        print("All dates are valid.")

def _validate(df: DataFrame,
              validators: List[Validator]):
    for v in validators:
        v.validate(df)

# def categorize(df: DataFrame, column: str) -> DataFrame:
#     df[column] = df[column].astype('category')
#     return df

class ActionBasedExtractor:
    def schema(self) -> List[str]: 
        pass
    def extract(self, df: DataFrame) -> DataFrame:
        pass
class TransactionExtractor(ActionBasedExtractor):
    def schema(self):
        return ['Action', 'Time', 'Notes', 'ID', 'Total', 'Currency (Total)']
    def extract(self, df: DataFrame) -> DataFrame:
        df = df[(df['Action'] == ActionType.Deposit.value) | (df['Action'] == ActionType.Withdrawal.value)]
        df = df[self.schema()]
        return df
class DividendExtractor(ActionBasedExtractor):
    def schema(self):
        return ['Action', 'Time', 'ISIN', 'Ticker', 'Name', 'No. of shares', 'Price / share', 'Currency (Price / share)', 'Exchange rate', 'Total', 'Currency (Total)', 'Withholding tax', 'Currency (Withholding tax)']
    def extract(self, df: DataFrame) -> DataFrame:
        df = df[(df['Action'] == ActionType.Dividend.value)]
        df = df[self.schema()]
        return df
class InterestOnCashExtractor(ActionBasedExtractor):
    def schema(self):
        return ['Action', 'Time', 'Notes', 'ID', 'Total', 'Currency (Total)']
    def extract(self, df: DataFrame) -> DataFrame:
        df = df[(df['Action'] == ActionType.InterestOnCash.value)]
        df = df[self.schema()]
        return df
class OrderExtractor(ActionBasedExtractor):
    def schema(self):
        return ['Action', 'Time', 'ISIN', 'Ticker', 'Name', 'ID', 'No. of shares', 'Price / share', 'Currency (Price / share)', 'Exchange rate', 'Total', 'Currency (Total)', 'Currency conversion fee', 'Currency (Currency conversion fee)']
    def extract(self, df: DataFrame) -> DataFrame:
        df = df[(df['Action'] == ActionType.MarketBuy.value) | 
                (df['Action'] == ActionType.MarketSell.value) | 
                (df['Action'] == ActionType.LimitBuy.value) |
                (df['Action'] == ActionType.LimitSell.value) |
                (df['Action'] == ActionType.StopSell.value) |
                (df['Action'] == ActionType.StopBuy.value) |
                (df['Action'] == ActionType.StopLimitBuy.value) |
                (df['Action'] == ActionType.StopLimitSell.value)]
        df = df[self.schema()]
        return df
class ActionBasedExtractorFactory:
    def get(self, type: HistoryType) -> ActionBasedExtractor:
        if type == HistoryType.Order:
            return OrderExtractor()
        elif type == HistoryType.Dividend:
            return DividendExtractor()
        elif type == HistoryType.Transaction:
            return TransactionExtractor()
        elif type == HistoryType.Interest:
            return InterestOnCashExtractor()
        raise ValueError("Unexpected history type: get {type}")

def extract(df: DataFrame, type: HistoryType) -> DataFrame:
    extractor = ActionBasedExtractorFactory().get(type)
    df = extractor.extract(df)
    return df

def save_csv(df: DataFrame, file_path: str):
    dst_dir = os.path.dirname(file_path)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved to {file_path}")

def main():
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    ingest_data_dir = os.path.join(src_dir, "resources/data/ingest")
    input_file = os.path.join(ingest_data_dir, "input/from_2024-03-24_to_2025-04-25.csv")
    output_dir = os.path.join(ingest_data_dir, "output")
    df = read_csv(input_file)

    transactions = extract(df, HistoryType.Transaction)
    save_csv(transactions, os.path.join(output_dir, "transactions.csv"))
    
    dividends = extract(df, HistoryType.Dividend)
    save_csv(dividends, os.path.join(output_dir, "dividends.csv"))
    
    interest = extract(df, HistoryType.Interest)
    save_csv(interest, os.path.join(output_dir, "interest.csv"))
    
    orders = extract(df, HistoryType.Order)
    save_csv(orders, os.path.join(output_dir, "orders.csv"))
    
    assert(sum(map(len, [transactions, dividends, interest, orders])) == len(df))

main()
