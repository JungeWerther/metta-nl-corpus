from datetime import datetime
from typing import List

import polars as pl
from dagster import AssetExecutionContext, Config, asset
from sqlmodel import Session, SQLModel, create_engine

from metta_nl_corpus.models.models import ProcessedExample, TextPair


class DatabaseConfig(Config):
    """Configuration for database connection."""

    database_url: str = "sqlite:///data.db"
    echo: bool = False


def get_db_engine(config: DatabaseConfig):
    """Get SQLite database engine."""
    return create_engine(config.database_url, echo=config.echo)


@asset
def database_setup(context: AssetExecutionContext, config: DatabaseConfig) -> None:
    """Initialize the database schema."""
    engine = get_db_engine(config)
    context.log.info("Creating database tables")

    try:
        SQLModel.metadata.create_all(engine)
        context.log.info("Database tables created successfully")
    except Exception as e:
        context.log.error(f"Failed to create database tables: {e}")
        raise


@asset
def text_pairs(
    context: AssetExecutionContext, preprocessed_training_data: pl.DataFrame
) -> List[TextPair]:
    """Transform preprocessed data into TextPair models."""
    context.log.info("Creating TextPair instances")

    # Validate required columns
    required_columns = {
        "premise_id",
        "hypothesis_id",
        "text_premise",
        "text_hypothesis",
    }
    missing_columns = required_columns - set(preprocessed_training_data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    pairs = []
    for row in preprocessed_training_data.iter_rows(named=True):
        try:
            pair = TextPair(
                premise_id=row["premise_id"],
                hypothesis_id=row["hypothesis_id"],
                premise_text=row["text_premise"],
                hypothesis_text=row["text_hypothesis"],
                language="nl",  # Assuming Dutch language based on project name
            )
            pairs.append(pair)
        except Exception as e:
            context.log.warning(f"Failed to create TextPair for row: {row}. Error: {e}")
            continue

    context.log.info(f"Created {len(pairs)} TextPair instances")
    return pairs


@asset
def processed_examples(
    context: AssetExecutionContext, text_pairs: List[TextPair]
) -> List[ProcessedExample]:
    """Create processed examples from text pairs with basic tokenization."""
    context.log.info("Creating ProcessedExample instances")

    def basic_tokenize(text: str) -> List[str]:
        """Very basic tokenization - split on whitespace and strip punctuation."""
        return [token.strip('.,!?()[]{}":;') for token in text.split()]

    examples = []
    for pair in text_pairs:
        try:
            example = ProcessedExample(
                text_pair_id=pair.id,
                premise_tokens=basic_tokenize(pair.premise_text),
                hypothesis_tokens=basic_tokenize(pair.hypothesis_text),
                metadata={
                    "language": pair.language,
                    "source": "metta-nl-corpus",
                    "processed_timestamp": datetime.utcnow().isoformat(),
                },
            )
            examples.append(example)
        except Exception as e:
            context.log.warning(f"Failed to process pair {pair.id}: {e}")
            continue

    context.log.info(f"Created {len(examples)} ProcessedExample instances")
    return examples


@asset
def store_text_pairs(
    context: AssetExecutionContext,
    config: DatabaseConfig,
    database_setup: None,  # Ensure database is ready
    text_pairs: List[TextPair],
) -> None:
    """Store text pairs in the database."""
    engine = get_db_engine(config)

    with Session(engine) as session:
        context.log.info(f"Storing {len(text_pairs)} text pairs")
        for pair in text_pairs:
            try:
                session.add(pair)
            except Exception as e:
                context.log.warning(f"Failed to store text pair {pair.id}: {e}")
                session.rollback()
                continue

        try:
            session.commit()
            context.log.info("Successfully stored all text pairs")
        except Exception as e:
            context.log.error(f"Failed to commit text pairs: {e}")
            session.rollback()
            raise


@asset
def store_processed_examples(
    context: AssetExecutionContext,
    config: DatabaseConfig,
    database_setup: None,  # Ensure database is ready
    processed_examples: List[ProcessedExample],
) -> None:
    """Store processed examples in the database."""
    engine = get_db_engine(config)

    with Session(engine) as session:
        context.log.info(f"Storing {len(processed_examples)} processed examples")
        batch_size = 1000

        for i in range(0, len(processed_examples), batch_size):
            batch = processed_examples[i : i + batch_size]
            try:
                for example in batch:
                    session.add(example)
                session.commit()
                context.log.info(f"Stored batch {i // batch_size + 1}")
            except Exception as e:
                context.log.error(f"Failed to store batch {i // batch_size + 1}: {e}")
                session.rollback()
                continue

        context.log.info("Completed storing all processed examples")
