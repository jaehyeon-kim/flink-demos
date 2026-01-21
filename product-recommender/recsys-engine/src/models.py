import argparse
import datetime
import dataclasses
import logging
from pathlib import Path
from collections import OrderedDict
from typing import List, Dict, Any

import pandas as pd
from faker import Faker

from src.location_generator import LocationGenerator
from src.utils import write_to_csv
from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class ModelMixin:
    @classmethod
    def from_dict(cls, data: dict):
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_rows(cls, rows: List[dict]):
        return [cls.from_dict(row) for row in rows]


@dataclasses.dataclass
class User(ModelMixin):
    user_id: int
    first_name: str
    last_name: str
    email: str
    age: int
    gender: str
    street_address: str
    postal_code: str
    city: str
    state: str
    country: str
    latitude: float
    longitude: float
    traffic_source: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def new(
        cls,
        *,
        user_id: int,
        loc_generator: LocationGenerator,
        fake: Faker,
    ):
        gender = fake.random_element(elements=("M", "F"))
        first_name = (
            fake.first_name_male() if gender == "M" else fake.first_name_female()
        )
        last_name = fake.last_name_nonbinary()
        location = loc_generator.get_one()
        traffic_source = fake.random_choices(
            elements=OrderedDict(
                zip(
                    ["Organic", "Facebook", "Search", "Email", "Display"],
                    [0.15, 0.06, 0.7, 0.05, 0.04],
                )
            ),
            length=1,
        )[0]
        return cls(
            user_id=user_id,
            first_name=first_name,
            last_name=last_name,
            email=f"{first_name.lower()}.{last_name.lower()}@{fake.safe_domain_name()}",
            age=fake.random_int(min=16, max=70),
            gender=gender,
            street_address=fake.street_address(),
            postal_code=location["postal_code"],
            city=location["city"],
            state=location["state"],
            country=location["country"],
            latitude=location["latitude"],
            longitude=location["longitude"],
            traffic_source=traffic_source,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
        )

    @staticmethod
    def generate_user_feature(user_obj, artifact: Dict[str, Any]):
        scaler = artifact["user_scaler"]
        expected_cols = artifact["user_columns"]

        # Create DataFrame
        df = pd.DataFrame([dataclasses.asdict(user_obj)])
        user_id = df["user_id"].values[0]

        # One-Hot Encode (Pandas will only create columns for existing values)
        cat_cols = ["gender", "traffic_source"]
        df_encoded = pd.get_dummies(df, columns=cat_cols, dtype=int)

        # Alignment - Reindex forces the DataFrame to match the training schema exactly.
        # 1. It adds missing columns (e.g., 'gender_F') filled with 0.
        # 2. It drops extra columns (e.g., 'first_name', 'city') automatically.
        df_aligned = df_encoded.reindex(columns=expected_cols, fill_value=0)

        # Scale Numerical Data
        # We must apply the scaler to the specific columns it was trained on
        num_cols = ["age", "latitude", "longitude"]
        df_aligned[num_cols] = scaler.transform(df_aligned[num_cols])

        # Re-attach ID
        df_aligned.insert(0, "user_id", user_id)
        return df_aligned.to_dict(orient="records")[0]

    @staticmethod
    def generate_synthetic_users(
        args: argparse.Namespace, data_path: Path, fake: Faker
    ):
        """Generates raw user profiles and saves to CSV."""
        logger.info(f"Generating {args.init_num_users} synthetic users...")

        loc_generator = LocationGenerator(
            country=args.country,
            state=args.state,
            city=args.city,
            postal_code=args.postal_code,
            fake=fake,
        )

        users = [
            dataclasses.asdict(
                User.new(user_id=i + 1, loc_generator=loc_generator, fake=fake)
            )
            for i in range(args.init_num_users)
        ]

        output_path = data_path / "users.csv"

        # Exclude metadata columns that are irrelevant for ML
        exclude_cols = ["created_at", "updated_at"]
        write_to_csv(users, output_path, exclude_cols)
        logger.info(f"Saved raw users to: {output_path}")
