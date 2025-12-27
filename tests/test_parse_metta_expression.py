from metta_nl_corpus.services.defs.transformation.assets import parse_metta_expression


class TestParseMettaExpression:
    """Test suite for parse_metta_expression function."""

    def test_simple_expression_without_code_blocks(self):
        """Test that simple expressions without code blocks are returned as-is."""
        expression = "(they-are-smiling-at-parents)"
        result = parse_metta_expression(expression)
        assert result == "(they-are-smiling-at-parents)"

    def test_expression_with_markdown_code_blocks(self):
        """Test removal of markdown code blocks with MeTTa label."""
        expression = """```MeTTa
(they-are-smiling-at-parents)
```"""
        result = parse_metta_expression(expression)
        assert result == "(they-are-smiling-at-parents)"

    def test_expression_with_plain_code_blocks(self):
        """Test removal of plain markdown code blocks."""
        expression = """```
(children smiling)
(children waving)
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)\n(children waving)"

    def test_multiline_expression_with_code_blocks(self):
        """Test multiline MeTTa expressions with code blocks."""
        expression = """```MeTTa
(children smiling)
(children waving)
(they-are-smiling-at-parents)
```"""
        result = parse_metta_expression(expression)
        assert (
            result
            == "(children smiling)\n(children waving)\n(they-are-smiling-at-parents)"
        )
        assert (
            result
            == "(children smiling)\n(children waving)\n(they-are-smiling-at-parents)"
        )

    def test_empty_string(self):
        """Test empty string input."""
        expression = ""
        result = parse_metta_expression(expression)
        assert result == ""

    def test_single_line_with_whitespace(self):
        """Test that trailing/leading whitespace is stripped."""
        expression = "   (children smiling)   "
        result = parse_metta_expression(expression)
        assert result == "(children smiling)"

    def test_code_blocks_with_whitespace(self):
        """Test code blocks with whitespace are properly handled."""
        expression = """```
   (children smiling)
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)"

    def test_multiline_with_empty_lines(self):
        """Test multiline expression with empty lines."""
        expression = """```
(children smiling)

(children waving)
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)\n\n(children waving)"

    def test_code_block_with_language_identifier(self):
        """Test code blocks with various language identifiers."""
        expression = """```metta
(children smiling)
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)"

    def test_nested_backticks_in_content(self):
        """Test expression containing backticks in the content."""
        expression = """```
(code-example "`children smiling`")
```"""
        result = parse_metta_expression(expression)
        assert result == '(code-example "`children smiling`")'

    def test_real_world_example_from_prompt(self):
        """Test with the real-world example from the user's prompt."""
        expression = """```MeTTa
(they-are-smiling-at-parents)
```"""
        result = parse_metta_expression(expression)
        assert result == "(they-are-smiling-at-parents)"

    def test_complex_metta_expression(self):
        """Test complex nested MeTTa expression."""
        expression = """```
(: find-evidence-for (-> Atom Atom))
(= (find-evidence-for $predicate)
   (match &evidence ($predicate $evidence) $evidence))
```"""
        result = parse_metta_expression(expression)
        expected = "(: find-evidence-for (-> Atom Atom))\n(= (find-evidence-for $predicate)\n   (match &evidence ($predicate $evidence) $evidence))"
        assert result == expected

    def test_expression_with_multiple_backtick_markers(self):
        """Test expression with multiple backtick markers in first/last line."""
        expression = "```python```metta\n(children smiling)\n```end```"
        result = parse_metta_expression(expression)
        # Both first and last lines contain backticks, so both should be removed
        assert result == "(children smiling)"

    def test_preserves_internal_formatting(self):
        """Test that internal formatting and indentation is preserved."""
        expression = """```
(= (process-data $input)
   (let $result
      (transform $input)
      (validate $result)))
```"""
        result = parse_metta_expression(expression)
        expected = "(= (process-data $input)\n   (let $result\n      (transform $input)\n      (validate $result)))"
        assert result == expected

    def test_extracts_code_block_from_lengthy_explanation(self):
        """Test extraction of MeTTa code block embedded in lengthy explanation text."""
        expression = """Okay, let's analyze the hypothesis: "They are smiling at their parents and generate a MeTTa expression that can be validated as a contradiction or not a contradiction with the previously generated MeTTa expression."

Given the previous output, we have:

`(children smiling)`
`(children waving)`

The hypothesis suggests a potential contradiction or inconsistency. Let's consider a few ways to approach this:

**1. Contradiction:**

If the output of `(children smiling)` and `(children waving)` *does* contradict the previous expression, that would be a contradiction. This is the most direct interpretation of the hypothesis.

**2. Non-Contradiction (Possible Variation):**

The hypothesis *could* be interpreted as a slightly modified statement. For example, it could be attempting to *assert* a specific relationship between the two, but it's not explicitly stating a conflict. It's more of a conditional statement or a scenario.

**3. Exploring potential variations:**

Let's consider potential refinements to the hypothesis. The hypothesis implies a logical *relationship* between the two. A valid example of this would be generating a statement that is not true.

**Proposed MeTTa Expression (Illustrative - based on the potential for a contradiction or non-contradiction):**

```MeTTa
(they-are-smiling-at-parents)
```

**Explanation:**

This expression simply states the condition – the children are smiling. It's a straightforward statement without any inherent conflict or contradiction that can be checked against the previous output. It doesn't attempt to validate a specific logical connection; it simply presents the condition.

**Important Note:** Without the previous MeTTa expression, this is a reasonable interpretation of the hypothesis based on its implication.

Therefore, the proposed expression `(they-are-smiling-at-parents)` directly reflects the suggested contradiction/non-contradiction based on the prior output."""
        result = parse_metta_expression(expression)
        assert result == "(they-are-smiling-at-parents)"

    def test_extracts_last_code_block_when_multiple_exist(self):
        """Test that only the last code block is extracted when multiple exist."""
        expression = """Here's the first expression:

```MeTTa
(first-expression)
```

And here's another one:

```MeTTa
(second-expression)
```"""
        result = parse_metta_expression(expression)
        assert result == "(second-expression)"

    def test_removes_comment_lines(self):
        """Test that lines starting with ; are removed."""
        expression = """```metta
; This is a comment
(children smiling)
; Another comment
(children waving)
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)\n(children waving)"

    def test_removes_indented_comment_lines(self):
        """Test that lines starting with ; after whitespace are removed."""
        expression = """```metta
(children smiling)
  ; This is an indented comment
(children waving)
    ; Another indented comment
```"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)\n(children waving)"

    def test_removes_comments_without_code_blocks(self):
        """Test that comments are removed even without code blocks."""
        expression = """; This is a comment
(children smiling)
; Another comment
(children waving)"""
        result = parse_metta_expression(expression)
        assert result == "(children smiling)\n(children waving)"

    def test_preserves_semicolons_within_expressions(self):
        """Test that semicolons within expressions are preserved."""
        expression = """```metta
(text "hello; world")
; This is a comment
(data "value; semicolon")
```"""
        result = parse_metta_expression(expression)
        assert result == '(text "hello; world")\n(data "value; semicolon")'

    def test_removes_comments_mixed_with_code(self):
        """Test removal of comments in complex code."""
        expression = """```metta
; Header comment
(= (process-data $input)
   ; Process the input
   (let $result
      ; Transform step
      (transform $input)
      ; Validation step
      (validate $result)))
; Footer comment
```"""
        result = parse_metta_expression(expression)
        expected = "(= (process-data $input)\n   (let $result\n      (transform $input)\n      (validate $result)))"
        assert result == expected
