"""
Router Test Suite - Tests routing accuracy across all LLM providers.

Reports results every 25 queries.
"""

import asyncio
import sys
import time
from typing import Dict, List, Any

sys.path.insert(0, '/Users/andyseaman/Notebooks/mcp_central')

# Test cases: (query, expected_tool, description)
TEST_CASES = [
    # Treasury & FRED
    ("10Y treasury rate", "get_treasury_rates", "Treasury rates"),
    ("yield curve", "get_treasury_rates", "Yield curve"),
    ("treasury rates", "get_treasury_rates", "Treasury rates explicit"),
    ("US inflation", "get_fred_series", "US inflation -> FRED"),
    ("inflation", "get_fred_series", "Inflation alone -> FRED"),
    ("unemployment rate", "get_fred_series", "Unemployment -> FRED"),
    ("US GDP", "get_fred_series", "US GDP -> FRED"),
    ("fed funds rate", "get_fred_series", "Fed funds -> FRED"),

    # NFA Ratings (disambiguation: "rating" -> NFA)
    ("Colombia NFA rating", "get_nfa_rating", "Explicit NFA rating"),
    ("Colombia rating", "get_nfa_rating", "Rating -> NFA"),
    ("Colombia", "get_nfa_rating", "Country alone -> NFA"),
    ("Brazil", "get_nfa_rating", "Country alone -> NFA"),
    ("Mexico rating", "get_nfa_rating", "Country rating -> NFA"),
    ("what's the rating for Indonesia", "get_nfa_rating", "Rating question -> NFA"),
    ("NFA for Turkey", "get_nfa_rating", "Explicit NFA"),
    ("ratings for G20", "get_nfa_batch", "Multiple ratings -> batch"),

    # Credit Ratings (explicit)
    ("Colombia credit rating", "get_credit_rating", "Credit rating explicit"),
    ("S&P rating for Brazil", "get_credit_rating", "S&P explicit"),
    ("Moody's rating Mexico", "get_credit_rating", "Moody's explicit"),

    # IMF Data
    ("Brazil GDP growth", "get_imf_indicator", "Country GDP -> IMF"),
    ("India inflation rate", "get_imf_indicator", "Country inflation -> IMF"),
    ("Germany current account", "get_imf_indicator", "Country CA -> IMF"),
    ("compare GDP US China", "compare_imf_countries", "Compare countries"),
    ("GDP growth comparison BRICS", "compare_imf_countries", "Compare group"),

    # World Bank
    ("India poverty rate", "get_worldbank_indicator", "Poverty -> WB"),
    ("Nigeria population", "get_worldbank_indicator", "Population -> WB"),
    ("country profile Kenya", "get_worldbank_country_profile", "Country profile"),

    # Portfolio
    ("my holdings", "get_client_holdings", "Holdings"),
    ("show holdings", "get_client_holdings", "Show holdings"),
    ("what do I own", "get_client_holdings", "What I own"),
    ("portfolio positions", "get_client_holdings", "Positions"),
    ("cash position", "get_portfolio_cash", "Cash"),
    ("available cash", "get_portfolio_cash", "Available cash"),
    ("transactions", "get_client_transactions", "Transactions"),
    ("trade history", "get_client_transactions", "Trade history"),
    ("watchlist", "get_watchlist", "Watchlist"),
    ("buy candidates", "get_watchlist", "Buy candidates"),

    # Compliance
    ("check compliance", "get_compliance_status", "Compliance"),
    ("UCITS status", "get_compliance_status", "UCITS"),
    ("5/10/40 check", "get_compliance_status", "5/10/40"),

    # Bonds
    ("search bonds", "search_bonds_rvm", "Search bonds"),
    ("bonds from Mexico", "search_bonds_rvm", "Bonds by country"),
    ("Brazilian bonds", "search_bonds_rvm", "Bonds by country adj"),
    ("find bonds BBB", "search_bonds_rvm", "Bonds by rating"),
    ("classify issuer PEMEX", "classify_issuer", "Classify issuer with name"),
    ("classify issuer", None, "Classify issuer alone -> clarification"),

    # ETF
    ("LQD ETF allocation", "get_etf_allocation", "ETF allocation with name"),
    ("LQD country exposure", "get_etf_country_exposure", "ETF exposure with name"),

    # Video
    ("video about inflation", "video_search", "Video topic search"),

    # Ambiguous (should ask for clarification)
    ("rating for", None, "Incomplete -> clarification"),
    ("compare", None, "Vague compare -> clarification"),
    ("data", None, "Too vague -> clarification"),
]


async def test_single_model(model_name: str, model_config: Dict) -> Dict[str, Any]:
    """Test a single model against all test cases."""
    from bob_mcp.src.fallback_client import FallbackLLMClient, ModelConfig
    from orca_mcp.tools.query_router import ROUTER_PROMPT
    import json

    results = {
        "model": model_name,
        "correct": 0,
        "incorrect": 0,
        "errors": 0,
        "details": []
    }

    # Create a client that only uses this model
    client = FallbackLLMClient(purpose="routing")
    client._initialized = True
    client._models = [ModelConfig(
        name=model_name,
        model_id=model_config["model_id"],
        provider=model_config["provider"],
        api_key_name=model_config["api_key"],
        priority=1
    )]

    system_prompt = "You are a tool router. Respond with valid JSON only, no other text."

    for i, (query, expected_tool, description) in enumerate(TEST_CASES):
        try:
            formatted_prompt = ROUTER_PROMPT.format(context="No prior context.", query=query)

            response = client.chat(
                messages=[{"role": "user", "content": formatted_prompt}],
                system=system_prompt,
                max_tokens=300
            )

            if not response.success:
                results["errors"] += 1
                results["details"].append({
                    "query": query,
                    "expected": expected_tool,
                    "got": "ERROR",
                    "error": response.error
                })
                continue

            # Parse response
            result_text = response.text.strip()
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]
                result_text = result_text.strip()

            result = json.loads(result_text)
            got_tool = result.get("tool")

            # Check if correct
            if expected_tool is None:
                # Should have asked for clarification
                is_correct = got_tool is None and "clarification" in result
            else:
                is_correct = got_tool == expected_tool

            if is_correct:
                results["correct"] += 1
            else:
                results["incorrect"] += 1
                results["details"].append({
                    "query": query,
                    "expected": expected_tool,
                    "got": got_tool,
                    "description": description
                })

        except json.JSONDecodeError as e:
            results["errors"] += 1
            results["details"].append({
                "query": query,
                "expected": expected_tool,
                "got": "PARSE_ERROR",
                "error": str(e)
            })
        except Exception as e:
            results["errors"] += 1
            results["details"].append({
                "query": query,
                "expected": expected_tool,
                "got": "EXCEPTION",
                "error": str(e)
            })

        # Progress report every 25 queries
        if (i + 1) % 25 == 0:
            total = results["correct"] + results["incorrect"] + results["errors"]
            accuracy = results["correct"] / total * 100 if total > 0 else 0
            print(f"  [{model_name}] Progress: {i+1}/{len(TEST_CASES)} | Accuracy: {accuracy:.1f}%")

    return results


async def run_all_tests():
    """Run tests against all models."""

    # Model configurations
    MODELS = {
        "GEMINI_FLASH": {
            "model_id": "gemini-2.0-flash-lite",
            "provider": "google",
            "api_key": "GEMINI_API_KEY"
        },
        "OPENAI_MINI": {
            "model_id": "gpt-4o-mini",
            "provider": "openai",
            "api_key": "OPENAI_API_KEY"
        },
        "GROK_FAST": {
            "model_id": "grok-4-1-fast-non-reasoning-latest",
            "provider": "xai",
            "api_key": "XAI_API_KEY"
        },
        "CLAUDE_HAIKU": {
            "model_id": "claude-haiku-4-5",
            "provider": "anthropic",
            "api_key": "ANTHROPIC_API_KEY"
        }
    }

    print(f"=" * 60)
    print(f"ROUTER TEST SUITE - {len(TEST_CASES)} test cases")
    print(f"=" * 60)
    print()

    all_results = {}

    for model_name, model_config in MODELS.items():
        print(f"\nTesting {model_name} ({model_config['model_id']})...")
        print("-" * 40)

        start_time = time.time()
        results = await test_single_model(model_name, model_config)
        elapsed = time.time() - start_time

        all_results[model_name] = results

        total = results["correct"] + results["incorrect"] + results["errors"]
        accuracy = results["correct"] / total * 100 if total > 0 else 0

        print(f"\n{model_name} RESULTS:")
        print(f"  Correct:   {results['correct']}/{total}")
        print(f"  Incorrect: {results['incorrect']}")
        print(f"  Errors:    {results['errors']}")
        print(f"  Accuracy:  {accuracy:.1f}%")
        print(f"  Time:      {elapsed:.1f}s")

        if results["details"]:
            print(f"\n  Failures:")
            for d in results["details"][:5]:  # Show first 5
                print(f"    - \"{d['query']}\" expected {d['expected']}, got {d['got']}")
            if len(results["details"]) > 5:
                print(f"    ... and {len(results['details']) - 5} more")

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"\n{'Model':<20} {'Accuracy':<12} {'Correct':<10} {'Errors':<10}")
    print("-" * 52)

    for model_name, results in all_results.items():
        total = results["correct"] + results["incorrect"] + results["errors"]
        accuracy = results["correct"] / total * 100 if total > 0 else 0
        print(f"{model_name:<20} {accuracy:>6.1f}%      {results['correct']:<10} {results['errors']:<10}")

    return all_results


if __name__ == "__main__":
    asyncio.run(run_all_tests())
