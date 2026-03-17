from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
ANNOTATIONS_PATH = PROJECT_ROOT / "datasets" / "annotations.parquet"
ANNOTATIONS_DB_PATH = PROJECT_ROOT / "datasets" / "annotations.db"
VALIDATIONS_PATH = PROJECT_ROOT / "datasets" / "validations.parquet"
ANNOTATION_GUIDELINE_PATH = PROJECT_ROOT / "documentation" / "annotation_guideline.md"
CLEANED_ANNOTATIONS_PATH = PROJECT_ROOT / "datasets" / "cleaned_annotations.parquet"
UPPER_ONTOLOGY_PATH = (
    PROJECT_ROOT / "metta_nl_corpus" / "services" / "spaces" / "upper-ontology.metta"
)
