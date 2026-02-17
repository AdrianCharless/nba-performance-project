import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

CHECKS = [
    # Gold view should exist + have rows
    ("gold.player_features Has Rows",
     "SELECT COUNT(*) FROM gold.player_features;",
     lambda x: x is not None and x > 0),

    # No null keys in silver
    ("Silver Keys Not Null",
     """
     SELECT COUNT(*) FROM silver.player_game_logs
     WHERE season IS NULL OR game_id IS NULL OR player_id IS NULL;
     """,
     lambda x: x == 0),

    # Opponent ranks should be within a sane range
    ("Opponent Ranks Not Within Range",
     """
     SELECT COUNT(*) FROM gold.player_features
     WHERE opp_pts_allowed_rank IS NOT NULL AND (opp_pts_allowed_rank < 0 OR opp_pts_allowed_rank > 32);
     """,
     lambda x: x == 0),
]

def main():
    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])

    failures = []
    with engine.begin() as conn:
        for name, sql, predicate in CHECKS:
            val = conn.execute(text(sql)).scalar()
            ok = predicate(val)
            print(f"{'Successful' if ok else 'Error'} {name}: {val}")
            if not ok:
                failures.append((name, val))

    if failures:
        raise SystemExit(f"Quality checks failed: {failures}")

    print("All quality checks passed.")

if __name__ == "__main__":
    main()
