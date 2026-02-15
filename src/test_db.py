import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    db_url = os.environ["DATABASE_URL"]

    engine = create_engine(db_url)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;")).scalar()
        print("Connected to Postgres Successfully. Test Query Result:", result)

if __name__ == "__main__":
    main()