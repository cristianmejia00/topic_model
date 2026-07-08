# Subqueries

This file tracks suggested step-06 subquery configurations.

Base context required by all step-06 scripts:

- SNAPSHOT (e.g. `2026-06-26`)
- QUERY (e.g. `q20260629`)
- SUBQUERY (per entry below)

Derived database name:

- `snapshot_{SNAPSHOT}-{QUERY}`

Canonical step-06 output root per subquery:

- `s3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/network/clustering/subqueries/{SUBQUERY}/`

## Quantum Computing

⚠️ Used as a test case.  
From `q20260629` parent set

**QUERY PARAMETERS**

SUBQUERY     = "quantum_computing"          # S3 subfolder name for this query
QUERY_TEXT   = "quantum_computing: Quantum computing architectures, superconducting transmons and trapped ion qubits, fault-tolerant quantum error correction (QEC) surface codes, NISQ algorithms like VQE and QAOA, quantum superposition, entanglement, and software SDKs like Qiskit and Cirq."          # may be a full paragraph
THRESHOLD    = 0.50                         # min cosine similarity to keep
MIN_SIZE     = 30                           # min papers per micro cluster

---

## Advanced Preemptive Medicine

Used as a test case.  
From `q20260629` parent set. 

**QUERY PARAMETERS**

SUBQUERY     = "preemptive medicine"          # S3 subfolder name for this query
QUERY_TEXT   = "Advanced preemptive medicine and dynamic drug discovery platforms, Mebyo sub-clinical pre-disease state AI models, healthcare digital twins utilizing multimodal lifelog data and PHR networks, generative molecular design via medical LLMs, novel therapeutic modalities including nucleic acids, mRNA vaccines, and genome editing, high-throughput biofoundries, transdisciplinary clinical data harmonization, implementation science for personalized preventive medicine, ELSI in predictive healthcare artificial intelligence."
THRESHOLD    = 0.50                         # min cosine similarity to keep
MIN_SIZE     = 30                           # min papers per micro cluster