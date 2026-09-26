# Third-party software and model notices

## Qwen2.5-1.5B-Instruct-GGUF

- **Publisher:** Qwen / Alibaba Cloud
- **Model:** Qwen2.5-1.5B-Instruct, GGUF, Q4_K_M
- **Source:** <https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF>
- **Pinned repository revision:** `91cad51170dc346986eccefdc2dd33a9da36ead9`
- **File:** `qwen2.5-1.5b-instruct-q4_k_m.gguf`
- **Expected SHA-256:** `6a1a2eb6d15622bf3c96857206351ba97e1af16c30d7a74ee38970e434e9407e`
- **License:** Apache License 2.0; see the model repository for the license text
  and applicable notices.
- **Distribution:** fetched during the LLM Docker image build, not included in
  this source tree or source ZIP.

The project uses the model for short, locally generated chat assistance. Output
is probabilistic and should be reviewed by users.

## llama-cpp-python

- **Package:** `llama-cpp-python==0.3.35`
- **Source:** <https://github.com/abetlen/llama-cpp-python>
- **License:** MIT; see the upstream repository for the full license text.
- **Build:** the pinned CPU wheel index is configured in `requirements-llm.txt`.

## Hugging Face Hub client

- **Package:** `huggingface-hub==0.35.3`
- **Source:** <https://github.com/huggingface/huggingface_hub>
- **License:** Apache License 2.0; see the upstream repository for the full
  license text.
