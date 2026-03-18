import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
ANNOTATIONS_PATH = PROJECT_ROOT / "datasets" / "annotations.parquet"
ANNOTATIONS_DB_PATH = PROJECT_ROOT / "datasets" / "annotations.db"
VALIDATIONS_PATH = PROJECT_ROOT / "datasets" / "validations.parquet"
CLEANED_ANNOTATIONS_PATH = PROJECT_ROOT / "datasets" / "cleaned_annotations.parquet"

PROMPTS_DIR = PROJECT_ROOT / "documentation" / "prompts"


def _resolve_guideline_path() -> Path:
    """Resolve annotation guideline path.

    Precedence:
    1. ANNOTATION_GUIDELINE_PATH env var (absolute or relative to PROJECT_ROOT)
    2. Default: documentation/prompts/default.md → documentation/annotation_guideline.md
    """
    env = os.environ.get("ANNOTATION_GUIDELINE_PATH")
    if env:
        p = Path(env)
        return p if p.is_absolute() else PROJECT_ROOT / p
    default_prompt = PROMPTS_DIR / "default.md"
    if default_prompt.exists():
        return default_prompt
    return PROJECT_ROOT / "documentation" / "annotation_guideline.md"


ANNOTATION_GUIDELINE_PATH = _resolve_guideline_path()
UPPER_ONTOLOGY_PATH = (
    PROJECT_ROOT / "metta_nl_corpus" / "services" / "spaces" / "upper-ontology.metta"
)
