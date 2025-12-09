"""
Video Gateway for Orca MCP

Routes video intelligence requests to Minerva MCP.
Orca acts as the gateway - clients (like Jess) never know about Minerva.

Tools exposed:
- video_search: Search video transcripts
- video_list: List available videos
- video_synthesize: Generate answers from video content
- video_get_transcript: Get transcript for specific video
"""

import asyncio
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Path to Minerva MCP
MINERVA_MCP_PATH = Path("/Users/andyseaman/Notebooks/minerva-mcp")


class MinervaMCPClient:
    """
    Client for communicating with Minerva MCP.

    Uses subprocess to spawn Minerva MCP server and communicate via stdio.
    This keeps Minerva completely hidden from external callers.
    """

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self._initialized = False

    async def _call_minerva_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call a Minerva MCP tool directly using Python imports.

        For simplicity, we import and call Minerva's functions directly
        rather than going through MCP protocol (since they're on same machine).
        """
        try:
            # Add Minerva to path
            minerva_src = MINERVA_MCP_PATH / "src"
            if str(minerva_src) not in sys.path:
                sys.path.insert(0, str(minerva_src))

            # Import Minerva's core components
            from core.search_index import VideoSearchIndex
            from core.transcriber import TranscriptionEngine

            # Handle different tools
            if tool_name == "search_videos":
                return await self._search_videos(arguments)
            elif tool_name == "list_indexed_videos":
                return await self._list_videos()
            elif tool_name == "synthesize_answer":
                return await self._synthesize_answer(arguments)
            elif tool_name == "get_video_transcript":
                return await self._get_transcript(arguments)
            elif tool_name == "keyword_search":
                return await self._keyword_search(arguments)
            else:
                return {"error": f"Unknown tool: {tool_name}"}

        except Exception as e:
            logger.error(f"Minerva tool call failed: {e}")
            return {"error": str(e)}

    async def _search_videos(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute video search via Minerva's search index."""
        try:
            from core.search_index import VideoSearchIndex

            db_path = MINERVA_MCP_PATH / "data" / "search_index.db"

            # Try to get OpenAI key for semantic search
            openai_key = None
            try:
                sys.path.insert(0, str(Path(__file__).parent.parent.parent))
                from auth_mcp.auth_client import get_api_key
                openai_key = get_api_key('OPENAI_API_KEY')
            except:
                import os
                openai_key = os.getenv('OPENAI_API_KEY')

            search_index = VideoSearchIndex(
                db_path=str(db_path),
                openai_api_key=openai_key
            )

            query = args.get("query", "")
            max_results = args.get("max_results", 10)

            # Use hybrid search for best results
            results = search_index.hybrid_search(query, limit=max_results)

            # Format results
            formatted = []
            for r in results:
                formatted.append({
                    "video_id": r.get("video_id", ""),
                    "title": r.get("title", ""),
                    "text": r.get("text", "")[:500],
                    "start_time": r.get("start_time", 0),
                    "end_time": r.get("end_time", 0),
                    "timestamp": f"{int(r.get('start_time', 0) // 60)}:{int(r.get('start_time', 0) % 60):02d}",
                    "url": f"https://youtube.com/watch?v={r.get('video_id', '')}&t={int(r.get('start_time', 0))}s",
                    "score": r.get("hybrid_score", r.get("score", 0))
                })

            return {"results": formatted, "count": len(formatted)}

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {"error": str(e), "results": []}

    async def _list_videos(self) -> Dict[str, Any]:
        """List all indexed videos."""
        try:
            transcript_path = MINERVA_MCP_PATH / "data" / "transcripts"

            videos = []
            if transcript_path.exists():
                for f in transcript_path.glob("*.json"):
                    if "_chunks" in f.name:
                        continue

                    video_id = f.stem
                    try:
                        with open(f) as fp:
                            data = json.load(fp)

                        duration = data.get("audio_duration", 0)
                        videos.append({
                            "video_id": video_id,
                            "title": data.get("title", f"Video {video_id}"),
                            "duration": duration,
                            "duration_formatted": f"{int(duration // 60)}m {int(duration % 60)}s",
                            "url": f"https://youtube.com/watch?v={video_id}",
                            "chapters": len(data.get("chapters", []))
                        })
                    except Exception as e:
                        logger.warning(f"Error reading transcript {f}: {e}")

            return {"videos": videos, "count": len(videos)}

        except Exception as e:
            logger.error(f"List videos failed: {e}")
            return {"error": str(e), "videos": []}

    async def _synthesize_answer(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize answer from video segments using Claude."""
        try:
            # Get Anthropic client
            anthropic_key = None
            try:
                sys.path.insert(0, str(Path(__file__).parent.parent.parent))
                from auth_mcp.auth_client import get_api_key
                anthropic_key = get_api_key('ANTHROPIC_API_KEY')
            except:
                import os
                anthropic_key = os.getenv('ANTHROPIC_API_KEY')

            if not anthropic_key:
                return {"error": "ANTHROPIC_API_KEY not available"}

            from anthropic import Anthropic
            client = Anthropic(api_key=anthropic_key)

            query = args.get("query", "")
            video_results = args.get("video_results", [])
            tone = args.get("tone", "professional")

            if not video_results:
                return {"answer": f"No video content found for: {query}", "sources": []}

            # Build context
            context_parts = []
            for i, r in enumerate(video_results[:5], 1):
                context_parts.append(
                    f"[Source {i}] {r.get('title', 'Video')} at {r.get('timestamp', '0:00')}:\n"
                    f"{r.get('text', '')}"
                )

            context = "\n\n---\n\n".join(context_parts)

            prompt = f"""Based on the following video transcript excerpts, answer this question: "{query}"

{context}

Instructions:
- Synthesize the key insights that answer the question
- Include the specific video title and timestamp for each key point
- Be concise but comprehensive
- Write in a {tone} tone
- If excerpts don't fully answer the question, acknowledge what's missing"""

            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )

            return {
                "answer": response.content[0].text,
                "sources": [
                    {
                        "video_id": r.get("video_id"),
                        "title": r.get("title"),
                        "timestamp": r.get("timestamp"),
                        "url": r.get("url")
                    }
                    for r in video_results[:5]
                ]
            }

        except Exception as e:
            logger.error(f"Synthesis failed: {e}")
            return {"error": str(e)}

    async def _get_transcript(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get transcript for a specific video."""
        try:
            video_id = args.get("video_id", "")
            transcript_path = MINERVA_MCP_PATH / "data" / "transcripts" / f"{video_id}.json"

            if not transcript_path.exists():
                return {"error": f"Transcript not found for {video_id}"}

            with open(transcript_path) as f:
                data = json.load(f)

            return {
                "video_id": video_id,
                "title": data.get("title", ""),
                "transcript": data.get("full_text", ""),
                "segments": data.get("speakers", [])[:50],  # Limit segments
                "duration": data.get("audio_duration", 0)
            }

        except Exception as e:
            logger.error(f"Get transcript failed: {e}")
            return {"error": str(e)}

    async def _keyword_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Fast keyword search across transcripts."""
        try:
            query = args.get("query", "").lower()
            max_results = args.get("max_results", 10)

            transcript_path = MINERVA_MCP_PATH / "data" / "transcripts"
            results = []

            if transcript_path.exists():
                for f in transcript_path.glob("*.json"):
                    if "_chunks" in f.name:
                        continue

                    video_id = f.stem
                    try:
                        with open(f) as fp:
                            data = json.load(fp)

                        title = data.get("title", f"Video {video_id}")

                        # Search in speaker segments
                        for segment in data.get("speakers", []):
                            text = segment.get("text", "")
                            if query in text.lower():
                                start = segment.get("start", 0)
                                results.append({
                                    "video_id": video_id,
                                    "title": title,
                                    "text": text[:300],
                                    "start_time": start,
                                    "timestamp": f"{int(start // 60)}:{int(start % 60):02d}",
                                    "url": f"https://youtube.com/watch?v={video_id}&t={int(start)}s"
                                })

                                if len(results) >= max_results:
                                    break
                    except:
                        continue

                    if len(results) >= max_results:
                        break

            return {"results": results, "count": len(results)}

        except Exception as e:
            logger.error(f"Keyword search failed: {e}")
            return {"error": str(e), "results": []}


# Global client instance
_minerva_client: Optional[MinervaMCPClient] = None


def get_minerva_client() -> MinervaMCPClient:
    """Get or create Minerva client."""
    global _minerva_client
    if _minerva_client is None:
        _minerva_client = MinervaMCPClient()
    return _minerva_client


# =============================================================================
# Orca Tool Functions (exposed via server.py)
# =============================================================================

async def video_search(query: str, max_results: int = 10) -> Dict[str, Any]:
    """
    Search video transcripts for relevant content.

    Args:
        query: Search query text
        max_results: Maximum results to return

    Returns:
        Dict with 'results' list containing matching video segments
    """
    client = get_minerva_client()
    return await client._call_minerva_tool("search_videos", {
        "query": query,
        "max_results": max_results
    })


async def video_list() -> Dict[str, Any]:
    """
    List all available videos in the library.

    Returns:
        Dict with 'videos' list containing video metadata
    """
    client = get_minerva_client()
    return await client._call_minerva_tool("list_indexed_videos", {})


async def video_synthesize(
    query: str,
    video_results: List[Dict],
    tone: str = "professional"
) -> Dict[str, Any]:
    """
    Synthesize an answer from video search results.

    Args:
        query: The original question
        video_results: Results from video_search
        tone: Response tone (professional, casual, educational)

    Returns:
        Dict with 'answer' and 'sources'
    """
    client = get_minerva_client()
    return await client._call_minerva_tool("synthesize_answer", {
        "query": query,
        "video_results": video_results,
        "tone": tone
    })


async def video_get_transcript(video_id: str) -> Dict[str, Any]:
    """
    Get the transcript for a specific video.

    Args:
        video_id: YouTube video ID

    Returns:
        Dict with transcript text and segments
    """
    client = get_minerva_client()
    return await client._call_minerva_tool("get_video_transcript", {
        "video_id": video_id
    })


async def video_keyword_search(query: str, max_results: int = 10) -> Dict[str, Any]:
    """
    Fast keyword search across video transcripts.

    Args:
        query: Keyword or phrase to search
        max_results: Maximum results to return

    Returns:
        Dict with 'results' list
    """
    client = get_minerva_client()
    return await client._call_minerva_tool("keyword_search", {
        "query": query,
        "max_results": max_results
    })
