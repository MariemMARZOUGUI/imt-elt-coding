from unittest.mock import patch, MagicMock, ANY
import src.database as db

# ==========================
# Test get_engine()
# ==========================
def test_get_engine_returns_engine():
    engine = db.get_engine()
    assert hasattr(engine, "connect")

# ==========================
# Test test_connection() - Failure case
# ==========================
@patch("src.database.get_engine")
def test_test_connection_failure(mock_get_engine):
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = Exception("connection failed")
    mock_get_engine.return_value = mock_engine
    assert db.test_connection() is False

