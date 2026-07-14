"""ASGI entry point for the Fenic MCP research service."""

from __future__ import annotations

import fenic as fc

from services.fenic.bootstrap import create_session

session = create_session()
table_names = [
    table
    for table in (
        "observations",
        "article_snapshots",
        "scrape_runs",
        "story_signals",
        "article_embeddings",
        "event_clusters",
    )
    if table in session.catalog.list_tables()
]
if not table_names:
    raise RuntimeError("Fenic catalog is empty; run services/fenic/bootstrap.py first")

server = fc.create_mcp_server(
    session,
    server_name="BBC News Analyser",
    system_tools=fc.SystemToolConfig(
        table_names=table_names,
        tool_namespace="bbc_news",
        max_result_rows=200,
    ),
    concurrency_limit=4,
)
app = fc.run_mcp_server_asgi(server, stateless_http=True, path="/mcp")

if __name__ == "__main__":
    fc.run_mcp_server_sync(server, transport="http", host="0.0.0.0", port=7860)
