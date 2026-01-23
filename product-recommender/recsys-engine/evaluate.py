import argparse
import sys
import warnings
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split
from mab2rec import BanditRecommender, LearningPolicy, NeighborhoodPolicy
from mab2rec.pipeline import benchmark
from jurity.recommenders import BinaryRecoMetrics, RankingRecoMetrics

# Silence warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=UserWarning)

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(sys.modules["__main__"].__file__).parent / "src" / "data"


def standardize_ids(series):
    return pd.to_numeric(series, errors="coerce").fillna(-1).astype(int).astype(str)


def keep_numeric_and_id(df, id_col):
    numeric_cols = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    if id_col not in numeric_cols:
        numeric_cols.insert(0, id_col)
    return df[numeric_cols]


def main():
    parser = argparse.ArgumentParser(description="Evaluate Recommendation Models")
    parser.add_argument("--seed", type=int, default=1237, help="Random seed.")
    args = parser.parse_args()

    # Load Data
    df_log = pd.read_csv(DATA_DIR / "training_log.csv")
    df_items = pd.read_csv(DATA_DIR / "product_features.csv")

    # Clean Data (Standardize IDs)
    uid, pid, response = "event_id", "product_id", "response"
    df_log[uid] = standardize_ids(df_log[uid])
    df_log[pid] = standardize_ids(df_log[pid])
    df_items[pid] = standardize_ids(df_items[pid])

    # Create User Features (Context)
    #   - We must drop 'product_id' and 'response' so they aren't used as features (Leakage)
    feature_cols = [c for c in df_log.columns if c not in [pid, response]]
    df_user_features = df_log[feature_cols].copy()
    df_user_features = keep_numeric_and_id(df_user_features, uid)

    # Clean Item Features
    df_items = keep_numeric_and_id(df_items, pid)

    # Split Data
    # We split the interaction log by time (shuffle=False)
    train_df, test_df = train_test_split(df_log, test_size=0.2, shuffle=False)

    # Define Metrics List
    # Set click_column to 'score'
    #   - The benchmark() function internally renames 'response' column to 'score'
    #     before passing it to Jurity.
    metric_params = {
        "click_column": "score",
        "user_id_column": uid,
        "item_id_column": pid,
    }

    top_k_list = [5]
    metrics = []

    for k in top_k_list:
        metrics.append(BinaryRecoMetrics.AUC(**metric_params, k=k))
        metrics.append(BinaryRecoMetrics.CTR(**metric_params, k=k))
        metrics.append(RankingRecoMetrics.Precision(**metric_params, k=k))
        metrics.append(RankingRecoMetrics.Recall(**metric_params, k=k))

    # Define Candidates
    kwargs = {"top_k": 5, "seed": args.seed}
    candidates = {
        "Random": BanditRecommender(LearningPolicy.Random(), **kwargs),
        "Popularity": BanditRecommender(LearningPolicy.Popularity(), **kwargs),
        "LinGreedy": BanditRecommender(
            learning_policy=LearningPolicy.LinGreedy(epsilon=0.1), **kwargs
        ),
        "LinUCB": BanditRecommender(LearningPolicy.LinUCB(alpha=1.0), **kwargs),
        "LinTS": BanditRecommender(learning_policy=LearningPolicy.LinTS(), **kwargs),
        "ClustersTS": BanditRecommender(
            learning_policy=LearningPolicy.ThompsonSampling(),
            neighborhood_policy=NeighborhoodPolicy.Clusters(n_clusters=10),
            **kwargs,
        ),
    }

    print(f"{'Running Benchmark... (This trains and scores all models automatically)'}")

    # Run Benchmark
    reco_to_results, reco_to_metrics = benchmark(
        candidates,
        metrics=metrics,
        train_data=train_df,
        test_data=test_df,
        user_features=df_user_features,
        item_features=df_items,
        user_id_col=uid,
        item_id_col=pid,
        response_col=response,
    )

    # Print Results
    print("-" * 80)
    results_df = pd.DataFrame(reco_to_metrics).T  # Transpose: Models as Rows

    # Print the available metric names to know what to sort by
    print("Available Metrics:", results_df.columns.tolist())

    # Sort safely (check if 'AUC' or 'auc' exists)
    sort_col = "AUC" if "AUC" in results_df.columns else "auc"
    if sort_col in results_df.columns:
        print(results_df.sort_values(by=sort_col, ascending=False))
    else:
        print(results_df)

    print("-" * 80)


if __name__ == "__main__":
    main()
