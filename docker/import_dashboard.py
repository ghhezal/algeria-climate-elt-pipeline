"""Initialize Superset database connection and import exported dashboard."""

import os
import subprocess
from pathlib import Path
from superset.app import create_app

DATABASE_NAME = "Algeria Weather Analytics"
SQLALCHEMY_URI = "postgresql+psycopg2://db_user:db_password@db:5432/db"
ZIP_PATH = Path("/app/docker/dashboard/algeria_climate_dashboard.zip")

def configure_database():
    print(f"Ensuring database connection '{DATABASE_NAME}' exists...")
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        existing = db.session.query(Database).filter_by(database_name=DATABASE_NAME).first()
        if not existing:
            old_db = db.session.query(Database).filter_by(database_name="Weather Analytics").first()
            if old_db:
                old_db.database_name = DATABASE_NAME
                old_db.set_sqlalchemy_uri(SQLALCHEMY_URI)
                old_db.expose_in_sqllab = True
                db.session.commit()
                print(f"Renamed database connection to '{DATABASE_NAME}'.")
            else:
                database = Database(
                    database_name=DATABASE_NAME,
                    sqlalchemy_uri=SQLALCHEMY_URI,
                    expose_in_sqllab=True,
                    allow_run_async=False,
                    allow_ctas=True,
                    allow_cvas=True,
                    allow_dml=False,
                    allow_file_upload=False,
                )
                db.session.add(database)
                db.session.commit()
                print(f"Successfully created database connection: '{DATABASE_NAME}'.")
        else:
            existing.set_sqlalchemy_uri(SQLALCHEMY_URI)
            existing.expose_in_sqllab = True
            db.session.commit()
            print(f"Database connection '{DATABASE_NAME}' already configured and up-to-date.")

def import_dashboard():
    if ZIP_PATH.exists():
        print(f"Importing dashboard package from {ZIP_PATH}...")
        result = subprocess.run(
            ["superset", "import-dashboards", "--path", str(ZIP_PATH), "--username", "admin"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("Dashboard imported successfully!")
            print(result.stdout)
        else:
            print(f"Warning: Failed to import dashboard (code {result.returncode}):")
            print(result.stderr)
            raise RuntimeError(f"superset import-dashboards failed: {result.stderr}")
    else:
        print(f"No dashboard zip found at {ZIP_PATH}, skipping import.")

if __name__ == "__main__":
    configure_database()
    import_dashboard()
