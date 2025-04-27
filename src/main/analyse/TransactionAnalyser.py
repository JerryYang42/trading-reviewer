import pandas as pd
from main.domain.models import TransactionType

class TransactionAnalyser:
    def __init__(self, path: str):
        self.transactions = pd.read_csv(path, header=0)

    def get_total_transactions(self):
        return len(self.transactions)

    def get_total_amount(self):
        total_withdrawal_amount = self.get_total_withdrawal_amount()
        total_deposit_amount = self.get_total_deposit_amount()
        total_amount = total_deposit_amount + total_withdrawal_amount
        assert total_amount >= 0, "Total deposit amount should be greater than or equal to total withdrawal amount"
        return total_amount

    def get_total_withdrawal_amount(self):
        return self._get_total_amount_by_action(TransactionType.Withdrawal)

    def get_total_deposit_amount(self):
        return self._get_total_amount_by_action(TransactionType.Deposit)
        
    def _get_total_amount_by_action(self, type: TransactionType):
        """Returns the total amount for a specific action type."""
        filtered = self._filter_by_action(type)
        return filtered['Total'].sum()

    def _filter_by_action(self, type: TransactionType):
        return self.transactions[self.transactions['Action'] == type.value]
