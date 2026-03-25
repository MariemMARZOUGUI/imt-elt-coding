"""
TP3 — Unit tests for src/extract.py
=====================================

These tests verify that extraction functions correctly read from S3
and load into Bronze, without needing real AWS or database connections.

We mock:
  - _get_s3_client → so we don't need real AWS credentials
  - _load_to_bronze → so we don't need a real database
  - _read_csv_from_s3 / _read_jsonl_from_s3 → to inject fake data
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.extract import (
    extract_products,
    extract_users,
    extract_orders,
    extract_order_line_items,
    extract_reviews,
    extract_clickstream,
    extract_all
)


class TestExtractProducts:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products
        result = extract_products()
        assert len(result) > 0
        mock_load.assert_called_once()   
          
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_s3_failure_raises(self, mock_read_csv, mock_load):
        mock_read_csv.side_effect = Exception("S3 timeout")
        with pytest.raises(Exception, match="S3 timeout"):
            extract_products()        

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        mock_read_csv.return_value = sample_products
        result = extract_products()
        assert isinstance(result, pd.DataFrame)


class TestExtractUsers:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        mock_read_csv.return_value = sample_users  
        result = extract_users()                   
        assert len(result) > 0
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_s3_failure_raises(self, mock_read_csv, mock_load):
        mock_read_csv.side_effect = Exception("S3 timeout")
        with pytest.raises(Exception, match="S3 timeout"):
            extract_users()
            
    @patch("src.extract._load_to_bronze")       
    @patch("src.extract._read_csv_from_s3")      
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_users):
        mock_read_csv.return_value = sample_users 
        result = extract_users() 
        assert isinstance(result, pd.DataFrame)


class TestExtractOrders:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        mock_read_csv.return_value = sample_orders  
        result = extract_orders()                   
        assert len(result) > 0
        mock_load.assert_called_once()
        
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_s3_failure_raises(self, mock_read_csv, mock_load):
        mock_read_csv.side_effect = Exception("S3 timeout")
        with pytest.raises(Exception, match="S3 timeout"):
            extract_orders()
            
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_orders):
        mock_read_csv.return_value = sample_orders  
        result = extract_orders() 
        assert isinstance(result, pd.DataFrame)

        
class TestExtractOrderLineItems:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_order_line_items):
        mock_read_csv.return_value = sample_order_line_items
        result = extract_order_line_items()
        assert len(result) > 0
        mock_load.assert_called_once()  
        
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_s3_failure_raises(self, mock_read_csv, mock_load):
        mock_read_csv.side_effect = Exception("S3 timeout")
        with pytest.raises(Exception, match="S3 timeout"):
            extract_order_line_items()           

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_order_line_items):
        mock_read_csv.return_value = sample_order_line_items
        result = extract_order_line_items()
        assert isinstance(result, pd.DataFrame)

class TestExtractReviews:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extracts_and_loads(self, mock_read_jsonl, mock_load, sample_reviews):
        mock_read_jsonl.return_value = sample_reviews  
        result = extract_reviews()
        assert len(result) > 0
        mock_load.assert_called_once()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_s3_failure_raises(self, mock_read_jsonl, mock_load):
        mock_read_jsonl.side_effect = Exception("S3 timeout")   
        with pytest.raises(Exception, match="S3 timeout"):
            extract_reviews()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_returns_dataframe(self, mock_read_jsonl, mock_load, sample_reviews):
        mock_read_jsonl.return_value = sample_reviews   
        result = extract_reviews()
        assert isinstance(result, pd.DataFrame)


class TestExtractClickStream:

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extracts_and_loads(self, mock_read_parquet, mock_load, sample_clickstream):
        mock_read_parquet.return_value = sample_clickstream   
        result = extract_clickstream()
        assert len(result) > 0
        mock_load.assert_called_once()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_s3_failure_raises(self, mock_read_parquet, mock_load):
        mock_read_parquet.side_effect = Exception("S3 timeout")   
        with pytest.raises(Exception, match="S3 timeout"):
            extract_clickstream()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_returns_dataframe(self, mock_read_parquet, mock_load, sample_clickstream): 
        mock_read_parquet.return_value = sample_clickstream   
        result = extract_clickstream()
        assert isinstance(result, pd.DataFrame)
class TestExtractAll:

    @patch("src.extract.extract_clickstream")
    @patch("src.extract.extract_reviews")
    @patch("src.extract.extract_order_line_items")
    @patch("src.extract.extract_orders")
    @patch("src.extract.extract_users")
    @patch("src.extract.extract_products")
    def test_calls_all_extractors(self, mock_products, mock_users, mock_orders,
        mock_line_items, mock_reviews, mock_clickstream):
        
        d = pd.DataFrame()
        mock_products.return_value = d
        mock_users.return_value = d
        mock_orders.return_value = d
        mock_line_items.return_value = d
        mock_reviews.return_value = d
        mock_clickstream.return_value = d

        
        result = extract_all()

        mock_products.assert_called_once()
        mock_users.assert_called_once()
        mock_orders.assert_called_once()
        mock_line_items.assert_called_once()
        mock_reviews.assert_called_once()
        mock_clickstream.assert_called_once()

    @patch("src.extract.extract_clickstream")
    @patch("src.extract.extract_reviews")
    @patch("src.extract.extract_order_line_items")
    @patch("src.extract.extract_orders")
    @patch("src.extract.extract_users")
    @patch("src.extract.extract_products")
    def test_returns_dict_with_6_keys(self, mock_products, mock_users, mock_orders,
        mock_line_items, mock_reviews, mock_clickstream):
        d = pd.DataFrame()
        for m in [mock_products, mock_users, mock_orders,
                  mock_line_items, mock_reviews, mock_clickstream]:
            m.return_value = d

        
        result = extract_all()

        assert isinstance(result, dict)
        assert set(result.keys()) == {
            "products", "users", "orders",
            "order_line_items", "reviews", "clickstream"
        }