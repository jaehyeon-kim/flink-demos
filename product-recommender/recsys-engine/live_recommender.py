import argparse
import logging
import pickle
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
from faker import Faker
from mabwiser.mab import MAB, LearningPolicy

from src.logger_config import setup_logger
from src.models import User
from src.bandit_simulator import TimeContextGenerator, GroundTruth

setup_logger()
logger = logging.getLogger(__name__)


class LiveRecommender:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.products = []
        self.product_features_map = {}
        self.artifact = {}

        # We cannot init MAB yet because we don't have 'arms' (products)
        self.model = None
        self.top_k = 5
        self.feature_schema = []  # To ensure column order consistency

    def load_artifacts(self):
        """Loads scaling artifacts and product features."""
        logger.info("Loading artifacts...")

        with open(self.data_path / "preprocessing_artifacts.pkl", "rb") as f:
            self.artifact = pickle.load(f)

        df_prod = pd.read_csv(self.data_path / "product_features.csv")
        self.products = df_prod["product_id"].tolist()
        self.product_features_map = df_prod.set_index("product_id").to_dict("index")

        logger.info(f"Loaded {len(self.products)} products.")

        # Define the Feature Schema (Column Order)
        user_cols = self.artifact["user_columns"]
        time_cols = [
            "is_morning",
            "is_afternoon",
            "is_evening",
            "is_weekend",
            "is_weekday",
        ]

        self.feature_schema = user_cols + time_cols

        # Initialize Engine (LinUCB)
        self.model = MAB(
            arms=self.products, learning_policy=LearningPolicy.LinUCB(alpha=1.0)
        )

    def warm_start(self):
        """Trains the model on historical CSV data (Offline Replay)."""
        logger.info("Warm starting model...")

        events_path = self.data_path / "event_features.csv"
        interactions_path = self.data_path / "interaction.csv"

        df_train = None

        if events_path.exists() and interactions_path.exists():
            df_events = pd.read_csv(events_path)
            df_inter = pd.read_csv(interactions_path)
            df_train = pd.merge(df_inter, df_events, on="event_id")

        if df_train is not None and not df_train.empty:
            decisions = df_train["product_id"].tolist()
            rewards = df_train["response"].tolist()
            contexts_df = df_train[self.feature_schema]

            self.model.fit(decisions=decisions, rewards=rewards, contexts=contexts_df)
            logger.info(f"Model warm-started on {len(decisions)} events.")

        else:
            logger.warning("History files not found or empty. Initializing Cold Start.")
            empty_contexts = pd.DataFrame(columns=self.feature_schema)
            self.model.fit(decisions=[], rewards=[], contexts=empty_contexts)

    def recommend(self, user_context: dict):
        clean_ctx = {k: v for k, v in user_context.items() if k != "user_id"}

        # Convert to DataFrame and Align Schema
        df_ctx = pd.DataFrame([clean_ctx])
        df_ctx = df_ctx.reindex(columns=self.feature_schema, fill_value=0)

        expectations = self.model.predict_expectations(contexts=df_ctx)

        # Handle Return Type (Dict vs List of Dicts)
        if isinstance(expectations, list):
            user_scores = expectations[0]
        else:
            user_scores = expectations

        sorted_arms = sorted(user_scores, key=user_scores.get, reverse=True)

        return sorted_arms[: self.top_k]

    def update(self, user_context: dict, item_id, reward):
        clean_ctx = {k: v for k, v in user_context.items() if k != "user_id"}

        df_ctx = pd.DataFrame([clean_ctx])
        df_ctx = df_ctx.reindex(columns=self.feature_schema, fill_value=0)

        self.model.partial_fit(decisions=[item_id], rewards=[reward], contexts=df_ctx)


def main():
    parser = argparse.ArgumentParser(description="Run Live Recommendation Loop")
    parser.add_argument("--data_dir", type=str, default="recsys-engine/src/data")
    parser.add_argument(
        "--steps", type=int, default=20, help="Number of users to simulate"
    )
    parser.add_argument("--seed", type=int, default=42)
    # Note: Location args are no longer needed for generation, but kept for compatibility
    parser.add_argument("--country", type=str, default="Australia")
    parser.add_argument("--state", type=str, default="*")
    parser.add_argument("--city", type=str, default="Melbourne")
    parser.add_argument("--postal-code", type=str, default="*")

    args = parser.parse_args()

    # Setup
    data_path = Path(args.data_dir)
    fake = Faker()
    Faker.seed(args.seed)
    random.seed(args.seed)

    # Load User Pool (from CSV)
    users_path = data_path / "users.csv"
    if not users_path.exists():
        logger.error(f"User file not found at {users_path}")
        return

    users_df = pd.read_csv(users_path)
    user_pool = users_df.to_dict("records")
    logger.info(f"Loaded {len(user_pool)} users")

    # Initialize Helpers
    time_gen = TimeContextGenerator(fake)
    ground_truth = GroundTruth()

    # Initialize Recommender
    recsys = LiveRecommender(data_path)
    recsys.load_artifacts()
    recsys.warm_start()

    print(f"\n--- STARTING LIVE LOOP ({args.steps} visits) ---\n")

    for i in range(args.steps):
        # Pick a Random User from Pool
        user_data = random.choice(user_pool)

        # Backfill mandatory fields for User dataclass (excluded in CSV)
        user_data["created_at"] = datetime.now()
        user_data["updated_at"] = datetime.now()

        # Rehydrate User Object
        user_obj = User.from_dict(user_data)

        # Randomize Visit Time (Simulate different times of day/week)
        # This fixes the issue of "same time" for every step
        simulated_time = fake.date_time_between(start_date="-7d", end_date="now")

        # Generate Features
        user_static_feats = User.generate_user_feature(user_obj, recsys.artifact)
        time_feats = time_gen.get_context(dt=simulated_time)

        # Combine Contexts
        full_context = {**user_static_feats, **time_feats}

        # Get Recommendation
        recommendations = recsys.recommend(full_context)

        # User Reaction (Simulation by Ground Truth)
        chosen_item = recommendations[0]
        reward = 0

        for item_id in recommendations:
            item_features = recsys.product_features_map[item_id]
            if ground_truth.will_click(full_context, item_features, fake):
                chosen_item = item_id
                reward = 1
                break

        # G. Update Model
        recsys.update(full_context, chosen_item, reward)

        # Log with the SIMULATED time
        print(
            f"User {str(user_obj.user_id).rjust(4, ' ')} ({user_obj.age} yo) @ {simulated_time.strftime('%a %H:%M')} "
            f"-> Recs: {recommendations} -> Clicked: {chosen_item} ({'YES' if reward else 'NO'})"
        )

    print("\n--- END LOOP ---")


if __name__ == "__main__":
    main()
