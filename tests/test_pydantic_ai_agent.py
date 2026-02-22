"""Tests for the Pydantic AI MeTTa expression generation agent."""

import pytest
from pydantic import ValidationError

from metta_nl_corpus.services.defs.transformation.assets import (
    AgentExpressionOutput,
    ExpressionDeps,
    RelationKind,
    _create_metta_agent,
    parse_all_tool,
    validate_relation_tool,
)
from metta_nl_corpus.constants import ANNOTATION_GUIDELINE_PATH
from pydantic_ai.models.test import TestModel


def test_parse_all_tool_success():
    """parse_all_tool returns success for valid MeTTa code."""
    result = parse_all_tool("(human Socrates)")
    assert result["success"] is True
    assert result["error"] is None


def test_parse_all_tool_failure():
    """parse_all_tool returns failure for invalid MeTTa code."""
    result = parse_all_tool("(unclosed")
    assert result["success"] is False
    assert result["error"] is not None


def test_agent_with_test_model():
    """Agent runs with TestModel and returns structured output."""
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, "openai:gpt-4o-mini")
    deps = ExpressionDeps(
        premise="Socrates is human.",
        hypothesis="A human exists.",
        label=RelationKind.ENTAILMENT,
    )
    test_model = TestModel(
        custom_output_args={
            "metta_premise": "(human Socrates)",
            "metta_hypothesis": "(human Socrates)",  # entails itself
            "relation": "entailment",
        }
    )

    with agent.override(model=test_model):
        result = agent.run_sync("Generate MeTTa expressions.", deps=deps)

    assert result.output is not None
    assert isinstance(result.output, AgentExpressionOutput)
    assert result.output.metta_premise == "(human Socrates)"
    assert result.output.metta_hypothesis == "(human Socrates)"
    assert result.output.relation == "entailment"


def test_agent_usage_present():
    """Agent result includes usage information."""
    system_prompt = ANNOTATION_GUIDELINE_PATH.read_text()
    agent = _create_metta_agent(system_prompt, "openai:gpt-4o-mini")
    deps = ExpressionDeps(
        premise="A cat is on the mat.",
        hypothesis="An animal is on the mat.",
        label=RelationKind.ENTAILMENT,
    )
    test_model = TestModel(
        custom_output_args={
            "metta_premise": "(cat the-cat) (on-mat the-cat)",
            "metta_hypothesis": "(cat the-cat) (on-mat the-cat)",  # entails itself
            "relation": "entailment",
        }
    )

    with agent.override(model=test_model):
        result = agent.run_sync("Generate MeTTa expressions.", deps=deps)

    usage = result.usage()
    assert hasattr(usage, "input_tokens") or hasattr(usage, "requests")


def test_validate_relation_tool_entailment():
    """validate_relation_tool returns valid=True for entailing expressions."""
    result = validate_relation_tool("(A B)", "(A B)", "entailment")
    assert result["valid"] is True
    assert "match" in result["message"]


def test_validate_relation_tool_invalid_relation():
    """validate_relation_tool returns valid=False for unknown relation."""
    result = validate_relation_tool("(A)", "(A)", "invalid")
    assert result["valid"] is False
    assert "Unknown relation" in result["message"]


def test_agent_expression_output_valid_entailment():
    """AgentExpressionOutput accepts valid entailing expressions."""
    output = AgentExpressionOutput(
        metta_premise="(A B)",
        metta_hypothesis="(A B)",
        relation="entailment",
    )
    assert output.relation == "entailment"


def test_agent_expression_output_rejects_invalid_relation():
    """AgentExpressionOutput raises when expressions don't match claimed relation."""
    with pytest.raises(ValidationError) as exc_info:
        AgentExpressionOutput(
            metta_premise="(A B)",
            metta_hypothesis="(is-not A B)",  # contradiction, not entailment
            relation="entailment",
        )
    assert "do not satisfy" in str(exc_info.value)
