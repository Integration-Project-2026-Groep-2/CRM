import pytest
from unittest.mock import AsyncMock, patch
from src.main import connect_with_retry

@pytest.mark.asyncio
async def test_connect_on_first_attempt():
    mock_connection = AsyncMock()
    with patch("src.main.get_rabbitmq_connection", return_value=mock_connection):
        result = await connect_with_retry("amqp://localhost/")
    assert result == mock_connection

@pytest.mark.asyncio
async def test_retries_on_failure_then_succeeds():
    mock_connection = AsyncMock()
    call_count = 0

    async def flakey_connect(url: str):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Connection failed")
        return mock_connection
    
    with patch("src.main.get_rabbitmq_connection", side_effect=flakey_connect), \
        patch("src.main.asyncio.sleep", new_callable=AsyncMock):  # Mock sleep to avoid actual delay
        result = await connect_with_retry("amqp://localhost/")
    
    assert result is mock_connection
    assert call_count == 2

@pytest.mark.asyncio
async def test_raises_after_max_attempts():
    with patch ("src.main.get_rabbitmq_connection", side_effect=ConnectionError("always fails")), \
        patch("src.main.asyncio.sleep", new_callable=AsyncMock): # Mock sleep to avoid actual delay
        with pytest.raises(ConnectionError, match="always fails"):
            await connect_with_retry("amqp://localhost/")