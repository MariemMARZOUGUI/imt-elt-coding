import pytest
import pandas as pd


@pytest.fixture
def sample_products():
    """Fake products DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "display_name": ["Nike Air Max", "Adidas Ultraboost", "Jordan 1"],
        "brand": ["Nike", "Adidas", "Jordan"],
        "category": ["sneakers", "sneakers", "sneakers"],
        "price_usd": [149.99, 179.99, -10.00], 
        "tags": ["running|casual", "running|boost", "retro|hype"],
        "is_active": [1, 1, 0],
        "is_hype_product": [0, 0, 1],
        "_internal_cost_usd": [50.0, 60.0, 70.0],
        "_supplier_id": ["SUP001", "SUP002", "SUP003"],
    })

@pytest.fixture
def sample_users():
    """Fake users DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "user_id": [1, 2],
        "email": [" Alice@Example.COM ", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Martin", "Smith"],
        "loyalty_tier": ["gold", None],
        "_hashed_password": ["abc123", "def456"],
        "_last_ip": ["1.2.3.4", "5.6.7.8"],
        "_device_fingerprint": ["fp1", "fp2"],
    })


@pytest.fixture
def sample_orders():
    """Fake orders DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 1],
        "order_date": ["2026-02-10", "2026-02-11", "2026-02-12"],
        "status": ["delivered", "shipped", "invalid_status"],
        "total_usd": [149.99, 179.99, 50.0],
        "coupon_code": ["SAVE10", None, None],
        "_stripe_charge_id": ["ch_1", "ch_2", "ch_3"],
        "_fraud_score": [0.1, 0.2, 0.9],
    })

@pytest.fixture
def sample_order_line_items():
    return pd.DataFrame({
        "line_item_id":          [1, 2, 3],
        "order_id":              [101, 101, 102],
        "product_id":            [10, 20, 30],
        "selected_size":         ["M", "L", "S"],
        "colorway":              ["black", "white", "red"],
        "quantity":              [1, 2, 1],
        "unit_price_usd":        [100.0, 50.0, 75.0],
        "line_total_usd":        [100.0, 100.0, 75.0],
        "_warehouse_id":         ["WH1", "WH2", "WH1"],
        "_internal_batch_code":  ["B001", "B002", "B003"],
        "_pick_slot":            ["A1", "B3", "C2"],
    })


@pytest.fixture
def sample_reviews():
    return pd.DataFrame({
        "review_id":          [1, 2, 3],
        "product_id":         [10, 20, 30],
        "user_id":            [1, 2, 3],
        "rating":             [5, 3, 1],
        "title":              ["Great!", "Average", "Terrible"],
        "body":               ["Love it", None, "Never again"],
        "verified_purchase":  [True, False, True],
        "moderation_status":  ["approved", "pending", "rejected"],
        "helpful_votes":      [10, 2, 0],
        "_sentiment_raw":     [0.9, 0.5, 0.1],
        "_toxicity_score":    [0.01, 0.1, 0.85],
    })


@pytest.fixture
def sample_clickstream():
    return pd.DataFrame({
        "event_id":             ["uuid-1", "uuid-2", "uuid-3"],
        "event_type":           ["pageview", "pageview", "pageview"],
        "timestamp":            pd.to_datetime(["2024-01-01 10:00", "2024-01-01 10:05", "2024-01-01 10:10"]),
        "user_id":              [1.0, None, 2.0],        
        "session_id":           ["sess-A", "sess-B", "sess-A"],
        "page_url":             ["https://shop.com/home", "https://shop.com/product/10", "https://shop.com/cart"],
        "page_path":            ["/home", "/product/10", "/cart"],
        "page_type":            ["home", "product", "cart"],
        "referrer_url":         ["https://google.com", None, "https://shop.com/home"],
        "referrer_source":      ["google", "direct", "internal"],
        "user_agent_raw":       ["Mozilla/5.0...", "Googlebot/2.1", "Mozilla/5.0..."],
        "ip_address":           ["1.2.3.4", "66.249.66.1", "5.6.7.8"],
        "viewport_width":       [1440, 800, 375],
        "viewport_height":      [900, 600, 812],
        "is_bot":               [False, True, False],
        "_ga_client_id":        ["GA1.1.xxx", "GA1.1.yyy", "GA1.1.zzz"],
        "_dom_interactive_ms":  [320.5, 150.0, 890.2],
        "_dom_complete_ms":     [450.0, 200.0, 1100.5],
        "_ttfb_ms":             [80.0, 40.0, 210.0],
    })