import argparse
import logging
import sys
from pathlib import Path

from faker import Faker

from src.models import User
from src.preprocessor import Preprocessor
from src.location_generator import LocationGenerator
from src.bandit_simulator import BanditSimulator
from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

# resolve paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(sys.modules["__main__"].__file__).parent / "src" / "data"


def main():
    parser = argparse.ArgumentParser(description="Initialize Data & Features")
    # --- General ---
    parser.add_argument("--seed", type=int, default=1237, help="Random seed.")
    # --- Data Generation (Users) ---
    parser.add_argument(
        "--init-num-users", type=int, default=1000, help="Number of initial users."
    )
    parser.add_argument(
        "--country", type=LocationGenerator.loc_prop, default="Australia"
    )
    parser.add_argument("--state", type=LocationGenerator.loc_prop, default="*")
    parser.add_argument("--city", type=LocationGenerator.loc_prop, default="Melbourne")
    parser.add_argument("--postal-code", type=LocationGenerator.loc_prop, default="*")
    # --- Data Preprocessing ---
    parser.add_argument(
        "--n-components", type=int, default=10, help="SVD components for text."
    )
    # --- Bandit Simulation ---
    parser.add_argument(
        "--n-past-events",
        type=int,
        default=10000,
        help="Number of past events to generate",
    )
    parser.add_argument(
        "--end-date-str",
        type=str,
        default="2026-01-01",
        help="Simulation event end date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    # Setup Randomness
    if args.seed is not None:
        Faker.seed(args.seed)
    fake = Faker()

    # Step 1: Generate Data
    User.generate_synthetic_users(args, DATA_DIR, fake)

    # Step 2: Create User/Product Features & Save Pipeline
    Preprocessor(data_path=DATA_DIR, n_components=args.n_components).run_preprocessing()

    # Step 3: Create Event Features & Simulate Interactions
    BanditSimulator(data_path=DATA_DIR, fake=fake).generate_history(
        num_events=args.n_past_events, end_date_str=args.end_date_str
    )

    logger.info("Data Preparation Complete.")


if __name__ == "__main__":
    main()
