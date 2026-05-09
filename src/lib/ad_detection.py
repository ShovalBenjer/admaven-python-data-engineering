"""
Ad detection module using heuristic analysis and LLM (Qwen) classification.

Provides both fast heuristic scoring and deep semantic analysis
for detecting advertising activity on web pages.
"""
import asyncio
import os
import re
import json
from typing import Dict
from huggingface_hub import InferenceClient
from tenacity import retry, stop_after_attempt, wait_exponential

# Ad signature keywords with confidence weights
AD_SIGNATURES = {
    'googlesyndication': 1.5,
    'doubleclick': 1.5,
    'prebid': 1.2,
    'criteo': 1.0,
    'adnxs': 1.0,
    'iframe': 0.2,
    'width="300"': 0.3,
    'height="250"': 0.3,
    'sponsored': 0.5,
}

HF_TOKEN = os.getenv('HF_TOKEN')


def heuristic_ad_detect(html: str) -> Dict[str, object]:
    """
    Analyzes HTML content using a weighted keyword dictionary to detect ad activity.
    Args:
        html (str): The raw HTML content of the page.
    Returns:
        Dict[str, object]: Dictionary containing 'is_running_ads' (bool) and 'ad_evidence' (str).
    """
    html_lower = html.lower()
    score = sum(weight for keyword, weight in AD_SIGNATURES.items() if keyword in html_lower)
    return {
        'is_running_ads': score > 2.0,
        'ad_evidence': f"Heuristic score: {score:.2f}",
        'heuristic_score': score,
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def detect_ads_with_qwen(html: str) -> Dict[str, object]:
    """
    Uses Qwen 2.5-72B via HuggingFace Inference API to semantically analyze HTML for ads.
    Args:
        html (str): The raw HTML content.
    Returns:
        Dict[str, object]: JSON analysis result with 'is_running_ads' and 'ad_evidence'.
    Raises:
        ValueError: If HF_TOKEN is missing or response invalid.
    """
    if not HF_TOKEN:
        raise ValueError("HF_TOKEN environment variable is required for LLM ad detection")
    
    client = InferenceClient(token=HF_TOKEN)
    prompt = (
        f"Analyze the following HTML content for advertising activity. "
        f"Consider ad networks, sponsored content, banner dimensions, and ad-related scripts. "
        f"Respond with ONLY a JSON object: {{'is_running_ads': true/false, 'ad_evidence': 'brief reason'}}.\n\n"
        f"HTML (first 2000 chars): {html[:2000]}"
    )
    
    def _call():
        return client.chat_completion(
            model="Qwen/Qwen2.5-72B-Instruct",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.1,
        )
    
    response = await asyncio.to_thread(_call)
    content = response.choices[0].message.content.strip()
    
    # Strip markdown code fences using regex (robust JSON parsing)
    content = re.sub(r'^```json\s*|\s*```$', '', content, flags=re.DOTALL).strip()
    
    try:
        result = json.loads(content)
        return {
            'is_running_ads': bool(result.get('is_running_ads', False)),
            'ad_evidence': result.get('ad_evidence', 'No explanation provided'),
            'llm_analysis': True,
        }
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Invalid LLM response format: {e}")


async def check_ads(html: str, use_llm: bool = True) -> Dict[str, object]:
    """
    Combined ad detection: heuristic first, then LLM if needed.
    Args:
        html: Raw HTML content
        use_llm: Whether to use LLM for uncertain cases
    Returns:
        Dict with detection results
    """
    # Quick heuristic pass
    heuristic = heuristic_ad_detect(html)
    
    if heuristic['is_running_ads']:
        return {
            **heuristic,
            'method': 'heuristic_confirm',
        }
    
    # If heuristic is uncertain and LLM is enabled, use LLM
    if use_llm and HF_TOKEN:
        try:
            llm_result = await detect_ads_with_qwen(html)
            return {
                **llm_result,
                'heuristic_score': heuristic.get('heuristic_score', 0),
            }
        except Exception as e:
            return {
                **heuristic,
                'method': 'heuristic_fallback',
                'llm_error': str(e),
            }
    
    return {
        **heuristic,
        'method': 'heuristic_only',
    }
