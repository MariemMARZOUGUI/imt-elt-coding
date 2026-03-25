import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.gold import create_customer_ltv, create_product_performance, create_daily_revenue, create_gold_layer

class TestGold_create_customer_ltv:
    @patch("src.gold._create_gold_table")
    @patch("src.gold.pd.read_sql")
    @patch("src.gold.get_engine")
    def test_create_customer_ltv(self, mock_engine, mock_read_sql, mock_create):
        fake_data = pd.DataFrame({"user_id": [1], "email": ["test@test.com"]})
        mock_read_sql.return_value = fake_data

        create_customer_ltv()

        assert mock_create.called
        args, _ = mock_create.call_args
        df_result = args[0]

        assert not df_result.empty
        assert "user_id" in df_result.columns
        assert "email" in df_result.columns


class TestGold_create_product_performance ():
    @patch("src.gold._create_gold_table")
    @patch("src.gold.pd.read_sql")
    @patch("src.gold.get_engine")
    def test_product_performance(self, mock_engine, mock_read_sql, mock_create):
        fake_data = pd.DataFrame({"product_id": [1], "product_name": [" product product"]})
        mock_read_sql.return_value = fake_data

        create_product_performance()

        args, _ = mock_create.call_args
        df_result = args[0]

        assert mock_create.called
        assert "product_id" in df_result.columns
        assert "product_name" in df_result.columns  

class TestGold_create_daily_revenue ():
    @patch("src.gold._create_gold_table")
    @patch("src.gold.pd.read_sql")
    @patch("src.gold.get_engine")
    def test_create_daily_revenue(self, mock_engine, mock_read_sql, mock_create):
        fake_data = pd.DataFrame({"order_date":[ 2026-3-25], "total_items": [2]})
        mock_read_sql.return_value = fake_data

        create_daily_revenue()

        args, _ = mock_create.call_args
        df_result = args[0]

        assert mock_create.called
        assert "order_date" in df_result.columns
        assert "total_items" in df_result.columns  

class TestGold_create_gold_layer():
    @patch("src.gold._create_gold_table")
    @patch("src.gold.pd.read_sql")
    @patch("src.gold.get_engine")
    def test_create_daily_revenue(self, mock_engine, mock_read_sql, mock_create):
        fake_data = pd.DataFrame({"product_id": [1]})
        mock_read_sql.return_value = fake_data

        create_gold_layer()

        assert mock_create.called





 