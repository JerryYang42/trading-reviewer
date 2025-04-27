import os
import sys
# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from TransactionAnalyser import TransactionAnalyser

src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ingest_data_dir = os.path.join(src_dir, "resources/data/ingest")
input_dir = os.path.join(ingest_data_dir, "output")

def analyse():
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    ingest_data_dir = os.path.join(src_dir, "resources/data/ingest")
    input_dir = os.path.join(ingest_data_dir, "output")
    transaction_csv = os.path.join(input_dir, "transactions.csv")

    transactionAnalyser = TransactionAnalyser(transaction_csv)
    total_amount = transactionAnalyser.get_total_amount()
    print("Total Amount:", total_amount)
    transactionAnalyser.get_total_deposit_amount()
    print("Total Deposit Amount:", transactionAnalyser.get_total_deposit_amount())
    transactionAnalyser.get_total_withdrawal_amount()
    print("Total Withdrawal Amount:", transactionAnalyser.get_total_withdrawal_amount())

analyse()