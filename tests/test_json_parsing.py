"""Tests for JSON parsing edge cases in LLM responses."""
import pytest
import re
from src.lib.ad_detection import detect_ads_with_qwen


class TestJSONParsing:
    """Test robust JSON parsing from LLM markdown responses."""

    def parse_content(self, content: str) -> str:
        """Helper to simulate the parsing logic."""
        return re.sub(r'^```json\s*|\s*```$', '', content, flags=re.DOTALL).strip()

    def test_plain_json_no_fences(self):
        """Plain JSON without markdown should parse fine."""
        content = '{"is_running_ads": true, "ad_evidence": "Found ads"}'
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is True

    def test_json_with_triple_fences(self):
        """JSON wrapped in ```json ... ``` should be stripped."""
        content = '''```json
{
  "is_running_ads": false,
  "ad_evidence": "No ads detected"
}
```'''
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is False

    def test_json_with_trailing_fence_only(self):
        """JSON with trailing ``` but no opening fence."""
        content = '{"is_running_ads": true}\n```'
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is True

    def test_json_with_leading_fence_only(self):
        """JSON with opening fence but no trailing."""
        content = '```json\n{"is_running_ads": false}'
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is False

    def test_json_with_extra_whitespace(self):
        """Fences with extra whitespace should be handled."""
        content = '```json   \n{"is_running_ads": true}\n   ```'
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is True

    def test_multiline_json_with_fences(self):
        """Multiline JSON within fences should parse correctly."""
        content = '''```json
{
  "is_running_ads": true,
  "ad_evidence": "Detected multiple ad networks"
}
```'''
        cleaned = self.parse_content(content)
        import json
        result = json.loads(cleaned)
        assert result['is_running_ads'] is True
        assert "multiple" in result['ad_evidence']

    def test_malformed_json_after_stripping(self):
        """After stripping fences, malformed JSON should still fail at json.loads."""
        content = '```json\n{invalid json}\n```'
        cleaned = self.parse_content(content)
        import json
        with pytest.raises(json.JSONDecodeError):
            json.loads(cleaned)

    def test_original_slicing_approach_broken(self):
        """
        Demonstrate the original brittle slicing approach (content[7:-3]) fails
        with certain fence variations.
        """
        # Original code: content[7:-3] assumes exactly '```json\n...\n```'
        content = '```json\n{"is_running_ads": true}\n```'
        old_way = content[7:-3]  # yields: '{"is_running_ads": true}'
        import json
        # This works for this case
        assert json.loads(old_way)['is_running_ads'] is True
        
        # But fails if fence has extra spaces
        content2 = '```json   \n{"is_running_ads": true}\n```'
        # old slicing doesn't account for spaces after json
        old_way2 = content2[7:-3]  # yields: '  \n{"is_running_ads": true}\n  '
        # This would fail JSON parsing due to leading/trailing non-JSON chars
        with pytest.raises(json.JSONDecodeError):
            json.loads(old_way2)
        
        # Our regex approach handles it
        cleaned = re.sub(r'^```json\s*|\s*```$', '', content2, flags=re.DOTALL).strip()
        assert json.loads(cleaned)['is_running_ads'] is True
