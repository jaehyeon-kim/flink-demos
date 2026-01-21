import logging
from pathlib import Path
from typing import List, Tuple

import pickle
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from textwiser import TextWiser, Embedding, Transformation

from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class Preprocessor:
    def __init__(self, data_path: Path, n_components: int):
        self.data_path = data_path
        self.users_path = self.data_path / "users.csv"
        self.products_path = self.data_path / "products.csv"
        self.n_components = n_components

    def preprocess_users(self) -> Tuple[pd.DataFrame, MinMaxScaler, List[str]]:
        """
        Pre-processes user features.

        Returns:
            - DataFrame of user features
            - Fitted Scaler (needed for online inference)
            - List of column names (needed for alignment)
        """
        # Load data
        df = pd.read_csv(self.users_path)

        # Extract ID (Save it before dropping columns)
        user_ids = df["user_id"]

        # Drop PII and unusable columns
        # As we use latitude and longitude, city/state/country/postal_code are redundant
        drop_cols = [
            "user_id",
            "first_name",
            "last_name",
            "email",
            "street_address",
            "city",
            "state",
            "country",
            "postal_code",
        ]
        df_clean = df.drop(columns=drop_cols, errors="ignore")

        # Encode Categorical Data (One-Hot Encoding)
        # dtype=int ensures we get 0/1 instead of False/True
        cat_cols = ["gender", "traffic_source"]
        df_encoded = pd.get_dummies(df_clean, columns=cat_cols, dtype=int)

        # Scale Numerical Data (Only real numbers)
        num_cols = ["age", "latitude", "longitude"]
        scaler = MinMaxScaler()
        df_encoded[num_cols] = scaler.fit_transform(df_encoded[num_cols])

        # Re-attach ID
        final_df = pd.concat([user_ids, df_encoded], axis=1)

        # Return the data AND the artifacts needed to process new users later
        return final_df, scaler, df_encoded.columns.tolist()

    def preprocess_products(
        self,
    ) -> Tuple[pd.DataFrame, TextWiser, MinMaxScaler, List[str]]:
        """
        Pre-processes product features.

        Returns:
            - DataFrame of product features
            - Fitted TextWiser model
            - Fitted Price Scaler
            - List of column names
        """
        # Load data
        df = pd.read_csv(self.products_path)

        # Safety: Fill missing text with empty strings
        df["name"] = df["name"].fillna("")
        df["description"] = df["description"].fillna("")

        ## Process Text Features
        # Combine name and description fields for richer context
        df["full_text"] = df["name"] + " " + df["description"]

        # Initialize and Fit TextWiser to Generate Embeddings
        # Strategy: TF-IDF (Count words) -> SVD (Reduce to top n meaningful patterns)
        tw = TextWiser(
            Embedding.TfIdf(), Transformation.SVD(n_components=self.n_components)
        )
        text_vectors = tw.fit_transform(df["full_text"])

        # Convert to DataFrame columns (text_0, text_1, etc.)
        text_df = pd.DataFrame(
            text_vectors,
            columns=[f"txt_{i}" for i in range(text_vectors.shape[1])],
            index=df.index,
        )

        ## Process Structured Features
        # One-Hot Encoding for categories
        cat_cols = ["category"]
        cat_df = pd.get_dummies(df[cat_cols], prefix="cat", dtype=int)

        # Scaling numerical features
        scaler = MinMaxScaler()
        num_cols = ["price"]
        num_df = pd.DataFrame(
            scaler.fit_transform(df[num_cols]), columns=num_cols, index=df.index
        )

        final_df = pd.concat([df["product_id"], text_df, cat_df, num_df], axis=1)

        return final_df, tw, scaler, final_df.columns.tolist()

    def run_preprocessing(self):
        """
        Converts raw CSVs into Feature Vectors (Matrices) for Bandits.
        Saves transformation artifacts (scalers) for online inference.
        """
        logger.info("Starting Feature Engineering...")

        # Process Users
        user_features, user_scaler, user_cols = self.preprocess_users()
        user_out_path = self.data_path / "user_features.csv"
        user_features.to_csv(user_out_path, index=False)
        logger.info(f"Saved User Features: {user_features.shape}")

        # Process Products
        # Assuming products.csv already exists in the data folder
        if not self.products_path.exists():
            logger.error(
                f"products.csv not found at {self.products_path}. Skipping product features."
            )
            return

        prod_features, tw_model, prod_scaler, prod_cols = self.preprocess_products()

        prod_out_path = self.data_path / "product_features.csv"
        prod_features.to_csv(prod_out_path, index=False)
        logger.info(f"Saved Product Features: {prod_features.shape}")

        # Save Artifacts for Inference
        # We must save these so the live demo can preprocess new users exactly the same way
        artifacts_path = self.data_path / "preprocessing_artifacts.pkl"
        with open(artifacts_path, "wb") as f:
            pickle.dump(
                {
                    "user_scaler": user_scaler,
                    "user_columns": user_cols,
                    "product_text_model": tw_model,
                    "product_price_scaler": prod_scaler,
                    "product_columns": prod_cols,
                },
                f,
            )

        logger.info(f"Saved Pipeline Artifacts to: {artifacts_path}")
