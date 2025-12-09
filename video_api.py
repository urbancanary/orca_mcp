"""
Orca Video API - HTTP wrapper for video intelligence tools.

This exposes Orca's video gateway as HTTP endpoints, allowing remote
clients (like Jess on Railway) to access video search and synthesis.

Run locally:
    uvicorn video_api:app --host 0.0.0.0 --port 8080

Endpoints:
    POST /video/search      - Search video transcripts
    GET  /video/list        - List available videos
    POST /video/synthesize  - Generate answer from video content
    GET  /video/transcript/{video_id} - Get specific transcript
    POST /video/keyword     - Fast keyword search
    GET  /health            - Health check
"""

import logging
import os
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Import Orca's video gateway functions
from tools.video_gateway import (
    video_search,
    video_list,
    video_synthesize,
    video_get_transcript,
    video_keyword_search
)

logger = logging.getLogger(__name__)

# =============================================================================
# Pydantic Models for Request/Response
# =============================================================================

class SearchRequest(BaseModel):
    """Request body for video search."""
    query: str = Field(..., description="Search query text")
    max_results: int = Field(10, description="Maximum results to return", ge=1, le=50)


class VideoResult(BaseModel):
    """A single video search result."""
    video_id: str
    title: str
    text: str
    start_time: float
    end_time: Optional[float] = None
    timestamp: str
    url: str
    score: Optional[float] = None


class SearchResponse(BaseModel):
    """Response from video search."""
    results: List[VideoResult]
    count: int


class VideoInfo(BaseModel):
    """Video metadata."""
    video_id: str
    title: str
    duration: float
    duration_formatted: str
    url: str
    chapters: int = 0


class ListResponse(BaseModel):
    """Response from video list."""
    videos: List[VideoInfo]
    count: int


class SynthesizeRequest(BaseModel):
    """Request body for synthesis."""
    query: str = Field(..., description="The question to answer")
    video_results: List[dict] = Field(..., description="Video search results to synthesize from")
    tone: str = Field("professional", description="Response tone: professional, casual, educational")


class SynthesizeResponse(BaseModel):
    """Response from synthesis."""
    answer: str
    sources: List[dict]


class TranscriptResponse(BaseModel):
    """Response from get transcript."""
    video_id: str
    title: str
    transcript: str
    segments: List[dict]
    duration: float


class KeywordRequest(BaseModel):
    """Request body for keyword search."""
    query: str = Field(..., description="Keyword or phrase to search")
    max_results: int = Field(10, description="Maximum results to return", ge=1, le=50)


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    version: str


# =============================================================================
# FastAPI App
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("Orca Video API starting up...")
    yield
    logger.info("Orca Video API shutting down...")


app = FastAPI(
    title="Orca Video API",
    description="HTTP API for video intelligence - search, synthesis, and transcript access",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        service="orca-video-api",
        version="1.0.0"
    )


@app.post("/video/search", response_model=SearchResponse)
async def search_videos(request: SearchRequest):
    """
    Search video transcripts for relevant content.

    Uses hybrid search (keyword + semantic) for best results.
    """
    try:
        result = await video_search(request.query, request.max_results)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return SearchResponse(
            results=[VideoResult(**r) for r in result.get("results", [])],
            count=result.get("count", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/video/list", response_model=ListResponse)
async def list_videos():
    """
    List all available videos in the library.

    Returns metadata for each indexed video.
    """
    try:
        result = await video_list()

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return ListResponse(
            videos=[VideoInfo(**v) for v in result.get("videos", [])],
            count=result.get("count", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/video/synthesize", response_model=SynthesizeResponse)
async def synthesize_answer(request: SynthesizeRequest):
    """
    Synthesize an answer from video search results.

    Uses Claude to generate a coherent response citing video timestamps.
    """
    try:
        result = await video_synthesize(
            request.query,
            request.video_results,
            request.tone
        )

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return SynthesizeResponse(
            answer=result.get("answer", ""),
            sources=result.get("sources", [])
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Synthesize failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/video/transcript/{video_id}", response_model=TranscriptResponse)
async def get_transcript(video_id: str):
    """
    Get the transcript for a specific video.

    Returns full transcript text and time-coded segments.
    """
    try:
        result = await video_get_transcript(video_id)

        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])

        return TranscriptResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get transcript failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/video/keyword", response_model=SearchResponse)
async def keyword_search(request: KeywordRequest):
    """
    Fast keyword search across video transcripts.

    Simpler than semantic search - finds exact keyword matches.
    """
    try:
        result = await video_keyword_search(request.query, request.max_results)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        # Keyword search results don't have score, add default
        results = []
        for r in result.get("results", []):
            r.setdefault("score", 1.0)
            r.setdefault("end_time", r.get("start_time", 0) + 30)
            results.append(VideoResult(**r))

        return SearchResponse(
            results=results,
            count=result.get("count", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Keyword search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Run with: uvicorn video_api:app --host 0.0.0.0 --port 8080
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
