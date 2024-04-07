import pytest
import asyncio
from producers.base import BaseProducer
from producers.klines_connector import KlinesConnector
from producer import main

@pytest.fixture
def mock_base_producer(mocker):
    return mocker.patch('producer.BaseProducer', autospec=True)

# @pytest.fixture
# def mock_klines_connector(mocker):
#     return mocker.patch('producer.KlinesConnector', autospec=True)


base_producer = BaseProducer()
producer = base_producer.start_producer()
connector = KlinesConnector(producer)
connector.start_stream()


def test_base_producer(mock_base_producer, mock_klines_connector):
    # Arrange
    mock_base_producer_instance = mock_base_producer.return_value
    mock_klines_connector_instance = mock_klines_connector.return_value

    # Act
    asyncio.run(main())

    # Assert
    mock_base_producer_instance.start_producer.assert_called_once()
    mock_klines_connector.assert_called_once_with(mock_base_producer_instance.start_producer.return_value)
    mock_klines_connector_instance.start_stream.assert_called_once()

def test_main_exception(mocker, mock_base_producer, mock_klines_connector):
    # Arrange
    mock_logging_error = mocker.patch('producer.logging.error')
    mock_base_producer.side_effect = Exception('Test exception')

    # Act
    with pytest.raises(Exception):
        asyncio.run(main())

    # Assert
    mock_logging_error.assert_called_once_with('Test exception')
