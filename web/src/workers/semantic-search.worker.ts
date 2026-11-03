import {
  buildOntologyLookup,
  rankSearchDocuments,
  type OntologyLookup,
  type SearchFilters,
  type SemanticDocument,
  type SemanticOntology,
} from "../lib/ontology-search";

interface SemanticIndex {
  model: string;
  dimensions: number;
  documentCount: number;
  vectorFile: string;
  documents: SemanticDocument[];
}

interface ProgressInfo {
  status?: string;
  file?: string;
  progress?: number;
  loaded?: number;
  total?: number;
}

interface TensorResult {
  data: ArrayLike<number>;
}

type Extractor = (
  text: string,
  options: { pooling: "mean"; normalize: true },
) => Promise<TensorResult>;

const QUERY_PREFIX = "Represent this sentence for searching relevant passages: ";
let documents: SemanticDocument[] = [];
let vectors = new Int8Array();
let dimensions = 0;
let extractor: Extractor | null = null;
let modelFailed = false;
let ontologyLookup: OntologyLookup = buildOntologyLookup(null);

function send(type: string, payload: Record<string, unknown> = {}) {
  self.postMessage({ type, ...payload });
}

function scoreQuery(query: ArrayLike<number>, index: number): number {
  const document = documents[index];
  const offset = index * dimensions;
  let dot = 0;
  let queryNorm = 0;
  for (let dimension = 0; dimension < dimensions; dimension += 1) {
    const queryValue = Number(query[dimension] ?? 0);
    dot += queryValue * Number(vectors[offset + dimension] ?? 0) * document.scale;
    queryNorm += queryValue * queryValue;
  }
  return dot / Math.max(Math.sqrt(queryNorm) * document.norm, Number.EPSILON);
}

function scoreDocuments(left: number, right: number): number {
  const leftDocument = documents[left];
  const rightDocument = documents[right];
  const leftOffset = left * dimensions;
  const rightOffset = right * dimensions;
  let dot = 0;
  for (let dimension = 0; dimension < dimensions; dimension += 1) {
    dot +=
      Number(vectors[leftOffset + dimension] ?? 0) *
      Number(vectors[rightOffset + dimension] ?? 0);
  }
  return (
    (dot * leftDocument.scale * rightDocument.scale) /
    Math.max(leftDocument.norm * rightDocument.norm, Number.EPSILON)
  );
}

async function loadIndex(metadataUrl: string, vectorsUrl: string, ontologyUrl?: string) {
  send("status", { phase: "index-loading", message: "Downloading the semantic index…" });
  const ontologyPromise = ontologyUrl
    ? fetch(ontologyUrl)
        .then((res) => (res.ok ? (res.json() as Promise<SemanticOntology>) : null))
        .catch(() => null)
    : Promise.resolve(null);

  const [metadataResponse, vectorsResponse, ontologyData] = await Promise.all([
    fetch(metadataUrl),
    fetch(vectorsUrl),
    ontologyPromise,
  ]);
  if (!metadataResponse.ok || !vectorsResponse.ok) {
    throw new Error("The semantic index could not be downloaded.");
  }
  const metadata = (await metadataResponse.json()) as SemanticIndex;
  const vectorBuffer = await vectorsResponse.arrayBuffer();
  documents = metadata.documents;
  dimensions = metadata.dimensions;
  vectors = new Int8Array(vectorBuffer);
  ontologyLookup = buildOntologyLookup(ontologyData);
  if (vectors.length !== documents.length * dimensions) {
    throw new Error("The semantic index metadata and vectors are out of sync.");
  }
  send("index-ready", {
    count: documents.length,
    model: metadata.model,
    topics: [...new Set(documents.map((document) => document.topic))].sort(),
    storyForms: [...new Set(documents.map((document) => document.story_form))].sort(),
    ontologyVersion: ontologyLookup.ontology.ontologyVersion,
  });
}

async function loadModel() {
  send("status", { phase: "model-loading", message: "Loading BGE Small for semantic queries…" });
  try {
    const { env, pipeline } = await import("@huggingface/transformers");
    if (env.backends.onnx.wasm) env.backends.onnx.wasm.numThreads = 1;
    extractor = (await pipeline("feature-extraction", "Xenova/bge-small-en-v1.5", {
      dtype: "q8",
      device: "wasm",
      progress_callback: (progress: ProgressInfo) => {
        send("model-progress", {
          file: progress.file,
          progress: progress.progress,
          loaded: progress.loaded,
          total: progress.total,
        });
      },
    })) as unknown as Extractor;
    send("model-ready", { message: "Semantic search ready" });
  } catch (error) {
    modelFailed = true;
    send("model-error", {
      message: error instanceof Error ? error.message : "The semantic model failed to load.",
    });
  }
}

async function runSearch(
  query: string,
  filters: SearchFilters,
  requestId: number,
  limit = 20,
) {
  if (!documents.length) throw new Error("The semantic index is not ready.");
  const semantic = extractor !== null;
  let queryVector: ArrayLike<number> | null = null;
  if (extractor) {
    const result = await extractor(`${QUERY_PREFIX}${query}`, {
      pooling: "mean",
      normalize: true,
    });
    queryVector = result.data;
  }
  const { results, ontologyContext } = rankSearchDocuments({
    query,
    documents,
    filters,
    lookup: ontologyLookup,
    semanticScoreAt: queryVector ? (index) => scoreQuery(queryVector, index) : undefined,
    limit,
  });
  send("search-results", {
    requestId,
    mode: semantic ? "semantic" : "keyword-fallback",
    results,
    ontologyContext,
  });
}

function related(storyId: string, requestId: number, limit = 6) {
  const selected = documents.findIndex((document) => document.story_id === storyId);
  if (selected < 0) {
    send("related-results", { requestId, storyId, selectedDocument: null, results: [] });
    return;
  }
  const results = documents
    .map((document, index) => ({ document, score: scoreDocuments(selected, index) }))
    .filter(({ document }, index) => index !== selected && document.story_id !== storyId)
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);
  send("related-results", {
    requestId,
    storyId,
    selectedDocument: documents[selected],
    results,
  });
}

self.addEventListener("message", (event: MessageEvent) => {
  const message = event.data as Record<string, unknown>;
  if (message.type === "init") {
    const ontologyUrl = message.ontologyUrl ? String(message.ontologyUrl) : undefined;
    void loadIndex(String(message.metadataUrl), String(message.vectorsUrl), ontologyUrl)
      .then(() => (message.loadModel === false ? undefined : loadModel()))
      .catch((error) => send("fatal-error", {
        message: error instanceof Error ? error.message : "Semantic search failed to start.",
      }));
    return;
  }
  if (message.type === "search") {
    void runSearch(
      String(message.query ?? ""),
      (message.filters ?? {}) as SearchFilters,
      Number(message.requestId ?? 0),
      Number(message.limit ?? 20),
    ).catch((error) => send("search-error", {
      requestId: Number(message.requestId ?? 0),
      message: error instanceof Error ? error.message : "Search failed.",
      modelFailed,
    }));
    return;
  }
  if (message.type === "related") {
    related(
      String(message.storyId ?? ""),
      Number(message.requestId ?? 0),
      Number(message.limit ?? 6),
    );
  }
});
