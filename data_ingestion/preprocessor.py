import pandas as pd
from sklearn.preprocessing import StandardScaler
from utils.logger import get_logger

logger = get_logger(__name__)

class DataPreprocessor:
    """
    Preprocesses data before further processing or analysis.
    """

    def __init__(self):
        self.scaler = StandardScaler()

    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies preprocessing steps to the input data.

        Args:
            data (pandas.DataFrame): Input data to be preprocessed.

        Returns:
            pandas.DataFrame: Preprocessed data.
        """
        logger.info("Preprocessing data...")
        data = self._handle_missing_values(data)
        data = self._normalize_data(data)
        logger.info("Data preprocessing complete.")
        return data

    def _handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing values in the data.
        """
        # Implement strategy for handling missing values
        # Example: filling with mean
        for col in data.columns:
            if data[col].isnull().any():
                if data[col].dtype == 'object':
                    data[col] = data[col].fillna(data[col].mode()[0])
                else:
                    data[col] = data[col].fillna(data[col].mean())
        return data

    def _normalize_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes numerical features in the data.
        """
        # Apply standard scaling for normalization
        for col in data.columns:
            if data[col].dtype != 'object':
                data[col] = self.scaler.fit_transform(data[col].values.reshape(-1, 1))
        return data

