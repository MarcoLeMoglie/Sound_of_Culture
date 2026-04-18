# Local Tooling Inventory

## Caveman Compression

- installed locally for Codex at `~/.codex/vendor/caveman-compression`
- source: `https://github.com/wilpel/caveman-compression`
- installation mode: local clone outside the project repo
- runtime: dedicated virtual environment at
  `~/.codex/vendor/caveman-compression/.venv`
- preferred mode: NLP-based offline compression

## Installed components

- repo cloned locally
- `requirements-nlp.txt` installed in the dedicated virtual environment
- attempted spaCy English model download twice
- both model-download attempts failed with upstream GitHub `504` errors
- local install patched with a blank English pipeline fallback so the tool
  remains usable without the downloadable model

## Example usage

```bash
/Users/marcolemoglie_1_2/.codex/vendor/caveman-compression/.venv/bin/python \
  /Users/marcolemoglie_1_2/.codex/vendor/caveman-compression/caveman_compress_nlp.py \
  compress "Verbose text here"
```

## Notes

- this install is local-only and intentionally not tracked in the project repo
- if GitHub model downloads recover later, rerun:

```bash
/Users/marcolemoglie_1_2/.codex/vendor/caveman-compression/.venv/bin/python \
  -m spacy download en_core_web_sm
```
