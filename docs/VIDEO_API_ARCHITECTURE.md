# Orca Video API Architecture

## Overview

The Orca Video API is an HTTP wrapper around Orca's video intelligence tools. It enables remote clients (like Jess running on Railway) to access video search and synthesis capabilities.

## Why HTTP?

Previously, Jess called Orca via direct Python imports:
```
Jess → import orca_mcp → Minerva files
```

This only worked when both were on the same machine. For deployment:
- **Railway** hosts the main app (with Jess)
- **Orca + Minerva** need to be deployed separately with access to video data

HTTP solves this:
```
Jess (Railway) ──HTTP──→ Orca Video API ──→ Minerva (search index, transcripts)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENTS                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐        ┌──────────────────┐                   │
│  │ Internal App │        │ Client SDK       │                   │
│  │ (Railway)    │        │ (minerva-jess)   │                   │
│  │              │        │                  │                   │
│  │ jess_agent   │        │ JessAgent        │                   │
│  └──────┬───────┘        └────────┬─────────┘                   │
│         │                         │                              │
│         └───────────┬─────────────┘                              │
│                     │ HTTP                                       │
│                     ▼                                            │
├─────────────────────────────────────────────────────────────────┤
│                     ORCA VIDEO API                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    FastAPI Server                         │   │
│  │                    (video_api.py)                         │   │
│  │                                                           │   │
│  │  POST /video/search     - Hybrid search                   │   │
│  │  GET  /video/list       - List videos                     │   │
│  │  POST /video/synthesize - Claude synthesis                │   │
│  │  GET  /video/transcript/{id} - Get transcript             │   │
│  │  POST /video/keyword    - Keyword search                  │   │
│  │  GET  /health           - Health check                    │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  video_gateway.py                         │   │
│  │                  (Minerva Client)                         │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
├─────────────────────────────┼────────────────────────────────────┤
│                     MINERVA (Hidden)                             │
├─────────────────────────────┼────────────────────────────────────┤
│                             ▼                                    │
│  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   │
│  │ search_index.db│   │  transcripts/  │   │   Anthropic    │   │
│  │  (SQLite FTS)  │   │  (JSON files)  │   │   (Claude)     │   │
│  └────────────────┘   └────────────────┘   └────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Deployment Options

### Option 1: Same Server (Development)
```
localhost:8080 - Orca Video API
```
Run: `uvicorn video_api:app --host 0.0.0.0 --port 8080`

### Option 2: Fly.io (Recommended for Production)
- Deploy Orca + Minerva data to Fly.io
- Persistent volume for search_index.db and transcripts
- URL: `https://orca-video.fly.dev`

### Option 3: Railway
- Deploy as separate Railway service
- Volume mount for data
- URL: `https://orca-video.up.railway.app`

### Option 4: Dedicated Server
- VPS with Docker
- Full control over resources
- URL: `https://orca.yourdomain.com`

## API Reference

### POST /video/search
Search video transcripts using hybrid search (keyword + semantic).

**Request:**
```json
{
    "query": "ASEAN governance",
    "max_results": 10
}
```

**Response:**
```json
{
    "results": [
        {
            "video_id": "AOVpTvMW6ro",
            "title": "Governance, Growth and Volatility: Navigating ASEAN",
            "text": "Governance is very key in the region...",
            "start_time": 65.5,
            "timestamp": "1:05",
            "url": "https://youtube.com/watch?v=AOVpTvMW6ro&t=65s",
            "score": 0.85
        }
    ],
    "count": 1
}
```

### GET /video/list
List all available videos.

**Response:**
```json
{
    "videos": [
        {
            "video_id": "AOVpTvMW6ro",
            "title": "Governance, Growth and Volatility: Navigating ASEAN",
            "duration": 245.5,
            "duration_formatted": "4m 5s",
            "url": "https://youtube.com/watch?v=AOVpTvMW6ro",
            "chapters": 3
        }
    ],
    "count": 1
}
```

### POST /video/synthesize
Generate an answer from video search results using Claude.

**Request:**
```json
{
    "query": "What are the governance challenges in ASEAN?",
    "video_results": [...],  // Results from /video/search
    "tone": "professional"
}
```

**Response:**
```json
{
    "answer": "Governance is a critical factor shaping investment outcomes across ASEAN...",
    "sources": [
        {
            "video_id": "AOVpTvMW6ro",
            "title": "Governance, Growth and Volatility",
            "timestamp": "1:05",
            "url": "https://youtube.com/watch?v=AOVpTvMW6ro&t=65s"
        }
    ]
}
```

### GET /video/transcript/{video_id}
Get full transcript for a specific video.

### POST /video/keyword
Fast keyword search (no semantic matching).

## Environment Variables

```env
# Required
ANTHROPIC_API_KEY=sk-ant-...    # For synthesis
OPENAI_API_KEY=sk-...           # For semantic search (optional)

# Optional
PORT=8080                        # Server port
CORS_ORIGINS=*                   # Allowed origins (comma-separated)
```

## Client Configuration

### Internal Jess (jess_agent.py)
```python
ORCA_API_URL = os.getenv("ORCA_API_URL", "http://localhost:8080")
```

### Client Jess (minerva-jess)
```env
ORCA_URL=https://orca-video.fly.dev
```

## Security Considerations

1. **API Authentication** (TODO)
   - Add API key authentication
   - `Authorization: Bearer <token>` header

2. **Rate Limiting** (TODO)
   - Protect synthesis endpoint (Claude costs)

3. **CORS**
   - Currently allows all origins
   - Restrict in production via `CORS_ORIGINS` env var

## Migration from Direct Imports

### Before (Direct Python Import)
```python
from orca_mcp.tools.video_gateway import video_search
result = await video_search(query, 10)
```

### After (HTTP)
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        f"{ORCA_API_URL}/video/search",
        json={"query": query, "max_results": 10}
    )
    result = response.json()
```

## Files

| File | Purpose |
|------|---------|
| `video_api.py` | FastAPI HTTP server |
| `tools/video_gateway.py` | Video tool implementations |
| `docs/VIDEO_API_ARCHITECTURE.md` | This documentation |
